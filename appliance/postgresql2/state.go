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
	"errors"
	"fmt"
	"reflect"
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
	RoleNone
)

type PgConfig struct {
	Role       Role
	Upstream   *discoverd.Instance
	Downstream *discoverd.Instance
}

func (x *PgConfig) Equal(y *PgConfig) bool {
	return reflect.DeepEqual(x, y)
}

type Postgres interface {
	XLogPosition() (xlog.Position, error)
	Reconfigure(*PgConfig) error
	Start() error
	Stop() error
}

type Peer struct {
	// Configuration
	id        string
	ident     *discoverd.Instance
	singleton bool

	// External Interfaces
	log       log15.Logger
	discoverd discoverd.Service
	postgres  Postgres

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

	// tick is used to trigger a state evaluation, usually in the future after
	// an error
	tick chan struct{}
}

func (p *Peer) Run() {
	for {
		select {
		// discoverd events
		// postgres events
		case <-p.tick:
		}
	}
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

// Examine the current cluster state and determine if new actions need to be
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

	// Bail out if we're configured for singleton mode but the cluster is not.
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

	presentPeers := make(map[string]struct{}, len(p.clusterPeers))
	presentPeers[p.clusterState.Primary.ID] = struct{}{}
	presentPeers[p.clusterState.Sync.ID] = struct{}{}

	newAsync := make([]*discoverd.Instance, 0, len(p.clusterPeers))
	changes := false

	for _, a := range p.clusterState.Async {
		if p.peerIsPresent(a) {
			presentPeers[a.ID] = struct{}{}
			newAsync = append(newAsync, a)
		} else {
			log.Debug("peer missing", "at", "missing_async", "async.id", a.ID, "async.addr", a.Addr)
			changes = true
		}
	}

	// Deposed peers should not be assigned as asyncs
	for _, d := range p.clusterState.Deposed {
		presentPeers[d.ID] = struct{}{}
	}

	for _, peer := range p.clusterPeers {
		if _, ok := presentPeers[peer.ID]; ok {
			continue
		}
		log.Debug("new peer", "at", "add_async", "async.id", peer.ID, "async.addr", peer.Addr)
		newAsync = append(newAsync, peer)
		changes = true
	}

	if !changes {
		if !noRest {
			p.rest()
		}
		return
	}

	p.startUpdateAsyncs(newAsync)
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
	log := p.log.New(log15.Ctx{"fn": "startInitialSetup"})
	log.Info("creating initial cluster state", "generation", 1, "at", "create_state")

	if err := p.putClusterState(); err != nil {
		log.Warn("failed to create cluster state", "at", "create_state", "err", err)
	} else {
		p.clusterState = p.updatingState
	}
	p.updating = false
	p.updatingState = nil

	go p.sendTick()
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

var (
	ErrClusterFrozen   = errors.New("cluster is frozen")
	ErrPostgresOffline = errors.New("postgres is offline")
	ErrPeerNotCaughtUp = errors.New("peer is not caught up")
)

func (p *Peer) startTakeoverWithPeer(reason string, minWAL xlog.Position, newState *State) (err error) {
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

	defer func() {
		if err == nil {
			return
		}
		p.updating = false
		p.updatingState = nil
		// In the event of an error, back off a bit and check state again in
		// a second. There are several transient failure modes that will resolve
		// themselves (e.g., postgres not yet online, postgres synchronous
		// replication not yet caught up).
		log.Warn("failed to declare new generation, backing off", "err", err)
		p.evalLater(1 * time.Second)
	}()

	if p.clusterState.Freeze != nil {
		return ErrClusterFrozen
	}

	// In order to declare a new generation, we'll need to fetch our current
	// transaction log position, which requires that postres be online. In most
	// cases, it will be, since we only declare a new generation as a primary or
	// a caught-up sync. During initial startup, however, we may find out
	// simultaneously that we're the primary or sync AND that the other is gone,
	// so we may attempt to declare a new generation before we've started
	// postgres. In this case, this step will fail, but we'll just skip the
	// takeover attempt until postgres is running. (Postgres coming online will
	// trigger another check of the cluster state that will trigger us to issue
	// another takeover if appropriate.)
	if !*p.pgOnline || p.pgTransitioning {
		return ErrPostgresOffline
	}
	wal, err := p.postgres.XLogPosition()
	if err != nil {
		return err
	}
	if x, err := xlog.Compare(wal, minWAL); err != nil || x < 0 {
		if err == nil {
			log.Warn("would attempt takeover but not caught up with primary yet",
				"at", "check_xlog",
				"want_wal", minWAL,
				"found_wal", wal,
			)
			err = ErrPeerNotCaughtUp
		}
		return err
	}
	p.updatingState.InitWAL = wal
	log.Info("declaring new generation")

	if err := p.putClusterState(); err != nil {
		return err
	}

	p.clusterState = p.updatingState
	p.updating = false
	p.updatingState = nil
	p.generation = p.clusterState.Generation
	log.Info("declared new generation",
		"at", "declared_generation",
		"generation", p.clusterState.Generation,
	)

	// assumePrimary() calls evalClusterState() to catch any
	// changes we missed while we were updating.
	p.assumePrimary()

	return nil
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

func (p *Peer) startUpdateAsyncs(newAsync []*discoverd.Instance) {
	if p.updating {
		panic("startUpdateAsyncs while already updating")
	}
	if p.updatingState != nil {
		panic("startUpdateAsyncs with existing update state")
	}
	log := p.log.New(log15.Ctx{"fn": "startUpdateAsyncs"})

	p.updating = true
	p.updatingState = &State{
		Generation: p.clusterState.Generation,
		Primary:    p.clusterState.Primary,
		Sync:       p.clusterState.Sync,
		Async:      newAsync,
		Deposed:    p.clusterState.Deposed,
		InitWAL:    p.clusterState.InitWAL,
	}
	log.Info("updating list of asyncs", "at", "update_state")
	err := p.putClusterState()
	if err != nil {
		log.Warn("failed to update cluster state", "err", err, "at", "update_state")
	} else {
		p.clusterState = p.updatingState
	}
	p.updating = false
	p.updatingState = nil

	go p.sendTick()
}

// Reconfigure postgres based on the current configuration. During
// reconfiguration, new requests to reconfigure will be ignored, and incoming
// cluster state changes will be recorded but otherwise ignored. When
// reconfiguration completes, if the desired configuration has changed, we'll
// take another lap to apply the updated configuration.
func (p *Peer) pgApplyConfig() error {
	log := p.log.New(log15.Ctx{"fn": "pgApplyConfig"})
	p.moving()

	if p.pgOnline == nil {
		panic("pgApplyConfig with postgres in unknown state")
	}
	if p.pgTransitioning {
		log.Info("skipping config apply, already transitioning", "at", "skip")
		return
	}

	config := p.pgConfig()
	if p.pgApplied != nil && p.pgApplied.Equal(config) {
		log.Info("skipping config apply, no changes", "at", "skip")
		return
	}

	p.pgTransitioning = true

	defer func() {
		// handle error
		/*
			 * This is a very unexpected error, and it's very
			 * unclear how to deal with it.  If we're the primary or
			 * sync, we might be tempted to abdicate our position.
			 * But without understanding the failure mode, there's
			 * no reason to believe any other peer is in a better
			 * position to deal with this, and we don't want to flap
			 * unnecessarily.  So just log an error and try again
			 * shortly.
			 *
			err = new VError(err, 'applying pg config');
			peer.mp_log.error(err);
			peer.mp_pg_retrypending = new Date();
			setTimeout(function retryPgApplyConfig() {
				peer.pgApplyConfig();
			}, 1000);
			return;
		*/
	}()

	log.Info("reconfiguring postgres")
	if err := p.postgres.Reconfigure(config); err != nil {
		return err
	}

	if config.Role != RoleNone {
		if *p.pgOnline {
			log.Debug("skipping start, already online", "at", "skip_start")
		} else {
			log.Debug("starting postgres", "at", "start")
			if err := p.postgres.Start(); err != nil {
				return err
			}
		}
	} else {
		if *p.pgOnline {
			log.Debug("stopping postgres", "at", "stop")
			if err := p.postgres.Stop(); err != nil {
				return err
			}
		} else {
			log.Debug("skipping stop, already offline", "at", "skip_stop")
		}
	}
	p.pgTransitioning = false

	/*
		peer.mp_log.info({ 'nretries': peer.mp_pg_nretries },
		    'pg: applied config', config);
	*/
	p.pgRetryPending = nil
	p.pgApplied = config
	p.pgOnline = config.Role != RoleNone

	// Try applying the configuration again in case anything's
	// changed.  If not, this will be a no-op.
	p.pgApplyConfig()
}

func (p *Peer) pgConfig() *PgConfig {
	switch p.role {
	case RolePrimary:
		return &PgConfig{Role: p.role, Downstream: p.clusterState.Sync}
	case RoleSync, RoleAsync:
		return &PgConfig{Role: p.role, Upstream: p.pgUpstream}
	case RoleUnassigned, RoleDeposed:
		return &PgConfig{Role: RoleNone}
	default:
		panic(fmt.Sprintf("unexpected role %v", p.role))
	}
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
	if err := p.discoverd.SetMeta(meta); err != nil {
		return err
	}
	p.clusterStateIndex = meta.Index
	return nil
}

// evalLater triggers a cluster state evaluation after delay has elapsed
func (p *Peer) evalLater(delay time.Duration) {
	time.AfterFunc(delay, p.sendTick)
}

// sendTick triggers a cluster state evaluation, it is usually called from
// a goroutine
func (p *Peer) sendTick() {
	p.tick <- struct{}{}
}
