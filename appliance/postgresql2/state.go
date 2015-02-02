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

	moving *time.Time
}

func (p *Peer) setMoving() {
	if p.moving == nil {
		p.log.Debug("moving", "fn", "moving")
		t := time.Now()
		p.moving = &t
	} else {
		p.log.Debug("already moving", "fn", "moving", "started_at", p.moving)
	}
}

func (p *Peer) evalClusterState(noRest bool) {
	log := p.log.New(log15.Ctx{"fn": "evalClusterState"})
	log.Info("starting state evaluation", "at", "start")

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
			// TODO this.rest()
		}

		return
	}

	if p.singleton && !p.clusterState.Singleton {
		shutdown.Fatal("configured for singleton mode but cluster found in normal mode")
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
}
