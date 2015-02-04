//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
//
// This file is derived from
// https://github.com/joyent/manatee-state-machine/blob/d441fe941faddb51d6e6237d792dd4d7fae64cc6/lib/manatee-peer.js
//
// Copyright (c) 2014, Joyent, Inc.
// Copyright (c) 2015, Prime Directive, Inc.
//

package state

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/flynn/flynn/appliance/postgresql2/xlog"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/shutdown"
	"github.com/inconshreveable/log15"
)

type State struct {
	Generation int                   `json:"generation"`
	Freeze     *FreezeDetails        `json:"freeze,omitempty"`
	Singleton  bool                  `json:"singleton,omitempty"`
	InitWAL    xlog.Position         `json:"init_wal"`
	Primary    *discoverd.Instance   `json:"primary"`
	Sync       *discoverd.Instance   `json:"sync"`
	Async      []*discoverd.Instance `json:"async"`
	Deposed    []*discoverd.Instance `json:"deposed,omitempty"`
}

type FreezeDetails struct {
	FrozenAt time.Time `json:"frozen_at"`
	Reason   string    `json:"reason"`
}

func NewFreezeDetails(reason string) *FreezeDetails {
	return &FreezeDetails{
		FrozenAt: time.Now().UTC(),
		Reason:   reason,
	}
}

type Role int

const (
	RoleUnknown Role = iota
	RolePrimary
	RoleSync
	RoleAsync
	RoleUnassigned
	RoleDeposed
)

type PgConfig struct {
	Role       Role
	Upstream   *discoverd.Instance
	Downstream *discoverd.Instance
}

type Peer struct {
	// Configuration
	id        string
	ident     *discoverd.Instance
	singleton bool

	// External Interfaces
	log       log15.Logger
	discoverd discoverd.Service
	postgres  interface{}

	// Dynamic state
	role          Role   // current role
	generation    int    // last generation updated processed
	updating      bool   // currently updating state
	updatingState *State // new state object

	clusterStateIndex uint64                // last known state index from discoverd
	clusterState      *State                // last received cluster state
	clusterPeers      []*discoverd.Instance // last received list of peers

	pgOnline        *bool     // nil for unknown
	pgSetup         bool      // whether db existed at start
	pgApplied       *PgConfig // last configuration applied
	pgTransitioning bool
	pgRetryPending  *time.Time
	pgRetryCount    int                 // consecutive failed retries
	pgUpstream      *discoverd.Instance // upstream replication target

	movingAt *time.Time
}

func (p *Peer) moving() {
	if p.movingAt == nil {
		p.log.Debug("moving", "fn", "moving")
		t := time.Now()
		p.movingAt = &t
	} else {
		p.log.Debug("already moving", "fn", "moving", "started_at", p.movingAt)
	}
}

func (p *Peer) rest() {
	// This is admittedly goofy, but it's possible to see cluster state change
	// events that would normally cause us to come to rest, but if there's still
	// a postgres transitioning happening, we're still moving. Similarly, if
	// we're in the middle of an update and we come to rest because a postgres
	// transition completed, then wait for the update to finish.
	if p.pgTransitioning || p.updating {
		return
	}

	p.movingAt = nil
	p.log.Debug("at rest", "fn", "rest")
	// TODO: this.emit('rest');
}

func (p *Peer) atRest() bool {
	return p.movingAt == nil
}

// Examine the current ZK cluster state and determine if new actions need to be
// taken.  For example, if we're the primary, and there's no sync present, then
// we need to declare a new generation.
//
// If noRest is true, then the state machine will be moving even if there's no
// more work to do here, so we should not issue a rest(). This is needed to
// avoid having callers think that we're coming to rest when we know we're not.
func (p *Peer) evalClusterState(noRest bool) {
	log := p.log.New(log15.Ctx{"fn": "evalClusterState"})
	log.Info("starting state evaluation", "at", "start")

	if p.atRest() {
		panic("evalClusterState when not moving")
	}

	// Ignore changes to the cluster state while we're in the middle of updating
	// it. When we finish updating the state (successfully or otherwise), we'll
	// check whether there was something important that we missed.
	if p.updating {
		log.Info("deferring state check (cluster is updating)", "at", "defer")
		return
	}

	// If there's no cluster state, check whether we should set up the cluster.
	// If not, wait for something else to happen.
	if p.clusterState == nil {
		log.Debug("no cluster state", "at", "no_state")

		if !p.pgSetup &&
			p.clusterPeers[0].ID == p.id &&
			(p.singleton || len(p.clusterPeers) > 1) {
			p.startInitialSetup()
		} else if p.role != RoleUnassigned {
			p.assumeUnassigned()
		} else if !noRest {
			p.rest()
		}

		return
	}

	// Bail out if we're configured for one-node-write mode but the cluster is
	// not.
	if p.singleton && !p.clusterState.Singleton {
		shutdown.Fatal("configured for singleton mode but cluster found in normal mode")
		return
	}

	// If the generation has changed, then go back to square one (unless we
	// think we're the primary but no longer are, in which case it's game over).
	// This may cause us to update our role and then trigger another call to
	// evalClusterState() to deal with any other changes required. We update
	// p.generation so that we know that we've handled the generation change
	// already.
	if p.generation != p.clusterState.Generation {
		p.generation = p.clusterState.Generation

		if p.role == RolePrimary {
			if p.clusterState.Primary.ID != p.id {
				p.assumeDeposed()
			} else if !noRest {
				p.rest()
			}
		} else {
			p.evalInitClusterState()
		}

		return
	}

	// Unassigned peers and async peers only need to watch their position in the
	// async peer list and reconfigure themselves as needed
	if p.role == RoleUnassigned {
		if i := p.whichAsync(); i != -1 {
			p.assumeAsync(i)
		} else if !noRest {
			p.rest()
		}
	}

	if p.role == RoleAsync {
		if whichAsync := p.whichAsync(); whichAsync == -1 {
			p.assumeUnassigned()
		} else {
			upstream := p.upstream(whichAsync)
			if upstream.ID != p.pgUpstream.ID {
				p.assumeAsync(whichAsync)
			} else if !noRest {
				p.rest()
			}
		}
		return
	}

	// The synchronous peer only needs to check the takeover condition, which is
	// that the primary has disappeared and the sync's WAL has caught up enough
	// to takeover as primary.
	if p.role == RoleSync {
		if !p.peerIsPresent(p.clusterState.Primary) {
			p.startTakeover("primary gone", p.clusterState.InitWAL)
		} else if !noRest {
			p.rest()
		}

		return
	}

	if p.role != RolePrimary {
		panic(fmt.Sprintf("unexpected role %v", p.role))
	}

	if !p.singleton && p.clusterState.Singleton {
		log.Info("configured for normal mode but found cluster in singleton mode, transitioning cluster to normal mode", "at", "transition_singleton")
		if p.clusterState.Primary.ID != p.id {
			panic(fmt.Sprintf("unexpected cluster state, we should be the primary, but %s is", p.clusterState.Primary.ID))
		}
		p.startTransitionToNormalMode()
		return
	}

	// The primary peer needs to check not just for liveness of the synchronous
	// peer, but also for other new or removed peers. We only do this in normal
	// mode, not one-node-write mode.
	if p.singleton {
		if !noRest {
			p.rest()
		}
		return
	}

	// TODO: It would be nice to process the async peers showing up and
	// disappearing as part of the same cluster state change update as our
	// takeover attempt. As long as we're not, though, we must handle the case
	// that we go to start a takeover, but we cannot proceed because there are
	// no asyncs. In that case, we want to go ahead and process the asyncs, then
	// consider a takeover the next time around. If we update this to handle
	// both operations at once, we can get rid of the goofy boolean returned by
	// startTakeover.
	if !p.peerIsPresent(p.clusterState.Sync) &&
		p.startTakeover("sync gone", p.clusterState.InitWAL) {
		return
	}

	/*
		presentpeers = {};
		presentpeers[zkstate.primary.id] = true;
		presentpeers[zkstate.sync.id] = true;
		newpeers = [];
		nchanges = 0;
		for (i = 0; i < zkstate.async.length; i++) {
			if (this.peerIsPresent(zkstate.async[i])) {
				presentpeers[zkstate.async[i].id] = true;
				newpeers.push(zkstate.async[i]);
			} else {
				this.mp_log.debug(zkstate.async[i], 'peer missing');
				nchanges++;
			}
		}

		/*
		 * Deposed peers should not be assigned as asyncs.
		 *
		for (i = 0; i < zkstate.deposed.length; i++)
			presentpeers[zkstate.deposed[i].id] = true;

		for (i = 0; i < this.mp_zkpeers.length; i++) {
			if (presentpeers.hasOwnProperty(this.mp_zkpeers[i].id))
				continue;

			this.mp_log.debug(this.mp_zkpeers[i], 'new peer found');
			newpeers.push(this.mp_zkpeers[i]);
			nchanges++;
		}

		if (nchanges === 0) {
			mod_assertplus.deepEqual(newpeers, zkstate.async);
			if (!norest)
				this.rest();
			return;
		}

		this.startUpdateAsyncs(newpeers);

	*/
}

func (p *Peer) startInitialSetup() {
	if p.updating {
		panic("already updating")
	}
	if p.updatingState != nil {
		panic("already have updating state")
	}

	p.updating = true
	p.updatingState = &State{
		Generation: 1,
		Primary:    p.ident,
		InitWAL:    xlog.Zero,
	}
	if p.singleton {
		p.updatingState.Singleton = true
		p.updatingState.Freeze = NewFreezeDetails("cluster started in singleton mode")
	} else {
		p.updatingState.Sync = p.clusterPeers[1]
		p.updatingState.Async = p.clusterPeers[2:]
	}

	/*
		this.mp_log.info(peer.mp_updating_state,
		    'creating initial cluster state');
		this.mp_zk.putClusterState(this.mp_updating_state, function (err) {
			peer.mp_updating = false;

			if (err) {
				err = new VError(err, 'failed to create cluster state');
				peer.mp_log.warn(err);
			} else {
				peer.mp_zkstate = peer.mp_updating_state;
				peer.mp_log.info('created cluster state');
			}

			peer.mp_updating_state = null;
			peer.evalClusterState();
		});
	*/
}

func (p *Peer) assumeUnassigned() {
	p.log.Info("assuming unassigned role", "role", "unassigned", "fn", "assumeUnassigned")
	p.role = RoleUnassigned
	p.pgUpstream = nil
	// TODO pgApplyConfig()
}

func (p *Peer) assumeDeposed() {
	p.log.Info("assuming deposed role", "role", "deposed", "fn", "assumeDeposed")
	p.role = RoleDeposed
	p.pgUpstream = nil
	// TODO pgApplyConfig()
}

func (p *Peer) assumePrimary() {
	p.log.Info("assuming primary role", "role", "primary", "fn", "assumePrimary")
	p.role = RolePrimary
	p.pgUpstream = nil

	// It simplifies things to say that evalClusterState() only deals with one
	// change at a time. Now that we've handled the change to become primary,
	// check for other changes.
	//
	// For example, we may have just read the initial state that identifies us
	// as the primary, and we may also discover that the synchronous peer is
	// not present. The first call to evalClusterState() will get us here, and
	// we call it again to check for the presence of the synchronous peer.
	//
	// We invoke pgApplyConfig() after evalClusterState(), though it may well
	// turn out that evalClusterState() kicked off an operation that will
	// change the desired postgres configuration. In that case, we'll end up
	// calling pgApplyConfig() again.
	p.evalClusterState(true)
	// TODO: pgApplyConfig()
}

func (p *Peer) assumeSync() {
	if p.singleton {
		panic("assumeSync as singleton")
	}
	p.log.Info("assuming sync role", "role", "sync", "fn", "assumeSync")

	p.role = RolePrimary
	p.pgUpstream = p.clusterState.Primary
	// See assumePrimary()
	p.evalClusterState(true)
	// TODO: pgApplyConfig()
}

func (p *Peer) assumeAsync(i int) {
	if p.singleton {
		panic("assumeAsync as singleton")
	}
	p.log.Info("assuming sync role", "role", "async", "fn", "assumeAsync")

	p.role = RoleAsync
	p.pgUpstream = p.upstream(i)

	// See assumePrimary(). We don't need to check the cluster state here
	// because there's never more than one thing to do when becoming the async
	// peer.
	// TODO: pgApplyConfig()
}

func (p *Peer) evalInitClusterState() {
	if p.updating {
		panic("already updating")
	}

	if p.clusterState.Primary.ID == p.id {
		p.assumePrimary()
		return
	}
	if p.clusterState.Singleton {
		p.assumeUnassigned()
		return
	}
	if p.clusterState.Sync.ID == p.id {
		p.assumeSync()
		return
	}

	for _, d := range p.clusterState.Deposed {
		if p.id == d.ID {
			p.assumeDeposed()
			return
		}
	}

	// If we're an async, figure out which one we are.
	if i := p.whichAsync(); i != -1 {
		p.assumeAsync(i)
		return
	}

	p.assumeUnassigned()
}

func (p *Peer) startTakeover(reason string, minWAL xlog.Position) bool {
	log := p.log.New(log15.Ctx{"fn": "startTakeover", "reason": reason, "min_wal": minWAL})

	// Select the first present async peer to be the next sync
	var newSync *discoverd.Instance
	for _, a := range p.clusterState.Async {
		if p.peerIsPresent(a) {
			newSync = a
			break
		}
	}
	if newSync == nil {
		log.Warn("would takeover but no async peers present", "at", "no_async")
		return false
	}

	log.Debug("preparing for new generation", "at", "prepare")
	newAsync := make([]*discoverd.Instance, 0, len(p.clusterState.Async))
	for _, a := range p.clusterState.Async {
		if a.ID != newSync.ID && p.peerIsPresent(a) {
			newAsync = append(newAsync, a)
		}
	}

	var newDeposed []*discoverd.Instance
	if p.clusterState.Primary.ID != p.id {
		newDeposed = make([]*discoverd.Instance, 0, len(p.clusterState.Deposed)+1)
		newDeposed = append(newDeposed, p.clusterState.Deposed...)
	} else {
		newDeposed = p.clusterState.Deposed
	}

	p.startTakeoverWithPeer(reason, minWAL, &State{
		Sync:    newSync,
		Async:   newAsync,
		Deposed: newDeposed,
	})
	return true

}

func (p *Peer) startTakeoverWithPeer(reason string, minWAL xlog.Position, newState *State) {
	log := p.log.New(log15.Ctx{"fn": "startTakeoverWithPeer", "reason": reason, "min_wal": minWAL})
	log.Info("starting takeover")

	if p.atRest() {
		panic("startTakeoverWithPeer while at rest")
	}

	// p.updating acts as a guard to prevent us from trying to make any other
	// changes while we're trying to write the new cluster state. If any state
	// change comes in while this is ongoing, we'll just record it and examine
	// it after this operation has completed (successfully or not).
	if p.updating {
		panic("startTakeoverWithPeer while already updating")
	}
	if p.updatingState != nil {
		panic("startTakeoverWithPeer with non-nil updatingState")
	}
	p.updating = true
	newState.Generation = p.clusterState.Generation + 1
	newState.Primary = p.ident
	p.updatingState = newState

	if p.updatingState.Primary.ID != p.clusterState.Primary.ID && len(p.updatingState.Deposed) == 0 {
		panic("startTakeoverWithPeer without deposing old primary")
	}

	/*
		mod_vasync.waterfall([
		    function takeoverCheckFrozen(callback) {
			/*
			 * We could have checked this above, but we want to take
			 * advantage of the code below that backs off for a second or
			 * two in this case and keeps mp_updating set.
			 *
			if (peer.mp_zkstate.freeze &&
			    peer.mp_zkstate.freeze !== null &&
			    peer.mp_zkstate.freeze !== false) {
				var err = new VError('cluster is frozen');
				err.name = 'ClusterFrozenError';
				callback(err);
			} else {
				callback();
			}
		    },
		    function takeoverFetchXlog(callback) {
			/*
			 * In order to declare a new generation, we'll need to fetch our
			 * current transaction log position, which requires that postres
			 * be online.  In most cases, it will be, since we only declare
			 * a new generation as a primary or a caught-up sync.  During
			 * initial startup, however, we may find out simultaneously that
			 * we're the primary or sync AND that the other is gone, so we
			 * may attempt to declare a new generation before we've started
			 * postgres.  In this case, this step will fail, but we'll just
			 * skip the takeover attempt until postgres is running.
			 * (Postgres coming online will trigger another check of the
			 * cluster state that will trigger us to issue another takeover
			 * if appropriate.)
			 *
			if (!peer.mp_pg_online || peer.mp_pg_transitioning) {
				var err = new VError('postgres is offline');
				err.name = 'PostgresOfflineError';
				callback(err);
			} else {
				peer.mp_pg.getXLogLocation(callback);
			}
		    },
		    function takeoverWriteState(wal, callback) {
			var error;

			if (minwal !== undefined &&
			    mod_xlog.xlogCompare(wal, minwal) < 0) {
				var err = new VError('would attempt takeover, but ' +
				    'not caught up to primary yet (want "%s", ' +
				    'found "%s"', minwal, wal);
				err.name = 'PeerNotCaughtUpError';
				callback(err);
				return;
			}

			peer.mp_updating_state.initWal = wal;
			error = mod_validation.validateZkState(peer.mp_updating_state);
			if (error instanceof Error)
				peer.fatal(error);
			peer.mp_log.info(peer.mp_updating_state,
			    'declaring new generation (%s)', reason);
			peer.mp_zk.putClusterState(
			    peer.mp_updating_state, callback);
		    }
		], function (err) {
			mod_assertplus.ok(!peer.atRest());

			/*
			 * In the event of an error, back off a bit and check state
			 * again in a few seconds.  There are several transient failure
			 * modes that will resolve themselves (e.g., postgres not yet
			 * online, postgres synchronous replication not yet caught up).
			 *
			if (err) {
				if (err.name == 'PeerNotCaughtUpError' ||
				    err.name == 'PostgresOfflineError' ||
				    err.name == 'ClusterFrozenError') {
					peer.mp_log.warn(err, 'backing off');
				} else {
					err = new VError('failed to declare ' +
					    'new generation');
					peer.mp_log.error(err, 'backing off');
				}

				setTimeout(function () {
					peer.moving();
					peer.mp_updating = false;
					peer.mp_updating_state = null;
					peer.evalClusterState();
				}, 1000);

				return;
			}

			peer.mp_zkstate = peer.mp_updating_state;
			peer.mp_updating_state = null;
			peer.mp_updating = false;
			peer.mp_gen = peer.mp_zkstate.generation;
			peer.mp_log.info('declared new generation');

			/*
			 * assumePrimary() calls evalClusterState() to catch any
			 * changes we missed while we were updating.
			 *
			peer.assumePrimary();
		});
	*/
}

// As the primary, converts the current cluster to normal mode from singleton
// mode.
func (p *Peer) startTransitionToNormalMode() {
	log := p.log.New(log15.Ctx{"fn": "startTransitionToNormalMode"})
	if p.clusterState.Primary.ID != p.id || p.role != RolePrimary {
		panic("startTransitionToNormalMode called when not primary")
	}

	// In the normal takeover case, we'd pick an async. In this case, we take
	// any other peer because we know none of them has anything replicated.
	var newSync *discoverd.Instance
	for _, peer := range p.clusterPeers {
		if peer.ID != p.id {
			newSync = peer
		}
	}
	if newSync == nil {
		log.Warn("would takeover but no peers present", "at", "no_sync")
		return
	}
	newAsync := make([]*discoverd.Instance, 0, len(p.clusterPeers))
	for _, a := range p.clusterPeers {
		if a.ID != p.id && a.ID != newSync.ID {
			newAsync = append(newAsync, a)
		}
	}

	log.Debug("transitioning to normal mode", "at", "takeover")
	p.startTakeoverWithPeer("transitioning to normal mode", xlog.Zero, &State{
		Sync:  newSync,
		Async: newAsync,
	})
}

// Determine our index in the async peer list. -1 means not present.
func (p *Peer) whichAsync() int {
	for i, a := range p.clusterState.Async {
		if p.id == a.ID {
			return i
		}
	}
	return -1
}

// Return the upstream peer for a given one of the async peers
func (p *Peer) upstream(whichAsync int) *discoverd.Instance {
	if whichAsync == 0 {
		return p.clusterState.Sync
	}
	return p.clusterState.Async[whichAsync-1]
}

// Returns true if the given other peer appears to be present in the most
// recently received list of present peers.
func (p *Peer) peerIsPresent(other *discoverd.Instance) bool {
	// We should never even be asking whether we're present.  If we need to
	// do this at some point in the future, we need to consider we should
	// always consider ourselves present or whether we should check the
	// list.
	if other.ID == p.id {
		panic("peerIsPresent with self")
	}
	for _, p := range p.clusterPeers {
		if p.ID == other.ID {
			return true
		}
	}

	return false
}

func (p *Peer) putClusterState() error {
	data, _ := json.Marshal(p.updatingState)
	meta := &discoverd.ServiceMeta{
		Data:  data,
		Index: p.clusterStateIndex,
	}
	if err := p.discoverd.SetServiceMeta(meta); err != nil {
		return err
	}
	p.clusterStateIndex = meta.Index
	return nil
}