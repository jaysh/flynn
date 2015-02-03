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
	"fmt"
	"time"

	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/shutdown"
	"github.com/inconshreveable/log15"
)

type State struct {
	Generation int    `json:"generation"`
	InitWAL    string `json:"init_wal"`
	Freeze     bool   `json:"freeze,omitempty"`
	Singleton  bool   `json:"singleton,omitempty"`

	Primary *discoverd.Instance   `json:"primary"`
	Sync    *discoverd.Instance   `json:"sync"`
	Async   []*discoverd.Instance `json:"async"`
	Deposed []*discoverd.Instance `json:"deposed,omitempty"`
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

	clusterState *State                // last received cluster state
	clusterPeers []*discoverd.Instance // last received list of peers

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

	if !p.atRest() {
		panic("unexpected evalClusterState while already moving")
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
			// TODO this.startInitialSetup()
		} else if p.role != RoleUnassigned {
			// TODO this.assumeUnassigned()
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
				// TODO: this.assumeDeposed()
			} else if !noRest {
				p.rest()
			}
		} else {
			// TODO: this.evalInitClusterState();
		}

		return
	}

	// Unassigned peers and async peers only need to watch their position in the
	// async peer list and reconfigure themselves as needed
	if p.role == RoleUnassigned {
		/*
			whichasync = this.whichAsync();
			if (whichasync != -1)
				this.assumeAsync(whichasync);
			else if (!norest)
				this.rest();
			return;
		*/
	}

	if p.role == RoleAsync {
		/*
			whichasync = this.whichAsync();
			if (whichasync == -1) {
				this.assumeUnassigned();
			} else {
				upstream = this.upstream(whichasync);
				if (upstream.id != this.mp_pg_upstream.id)
					this.assumeAsync(whichasync);
				else if (!norest)
					this.rest();
			}
			return;
		*/
	}

	// The synchronous peer only needs to check the takeover condition, which is
	// that the primary has disappeared and the sync's WAL has caught up enough
	// to takeover as primary.
	if p.role == RoleSync {
		/*
			if (!this.peerIsPresent(zkstate.primary))
				this.startTakeover('primary gone', zkstate.initWal);
			else if (!norest)
				this.rest();

			return;
		*/
	}

	if p.role != RolePrimary {
		panic(fmt.Sprintf("unexpected role %v", p.role))
	}

	if !p.singleton && p.clusterState.Singleton {
		log.Info("configured for normal mode but found cluster in singleton mode, transitioning cluster to normal mode", "at", "transition_singleton")
		if p.clusterState.Primary.ID != p.id {
			panic(fmt.Sprintf("unexpected cluster state, we should be the primary, but %s is", p.clusterState.Primary.ID))
		}
		// TODO: this.startTransitionToNormalMode()
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

	/*
		if (!this.peerIsPresent(zkstate.sync) &&
		    this.startTakeover('sync gone')) {
			return;
		}

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

func (p *Peer) startTakeover() {

}

func (p *Peer) startInitialSetup() {
	if p.updating {
		panic("already updating")
	}
	if p.updatingState != nil {
		panic("already have updating state")
	}

	p.updating = true
	p.updatingState = 
}
