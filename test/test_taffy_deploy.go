package main

import (
	"bytes"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/pkg/cluster"
)

type TaffyDeploySuite struct {
	Helper
}

var _ = c.Suite(&TaffyDeploySuite{})

func (s *TaffyDeploySuite) TestDeploys(t *c.C) {
	client := s.controllerClient(t)

	github := map[string]string{
		"user_login": "flynn-examples",
		"repo_name":  "go-flynn-example",
		"ref":        "master",
		"sha":        "a2ac6b059e1359d0e974636935fda8995de02b16",
		"clone_url":  "https://github.com/flynn-examples/go-flynn-example.git",
	}

	// initial deploy

	app := &ct.App{
		Meta: map[string]string{
			"type":       "github",
			"user_login": github["user_login"],
			"repo_name":  github["repo_name"],
			"ref":        github["ref"],
			"sha":        github["sha"],
			"clone_url":  github["clone_url"],
		},
	}
	t.Assert(client.CreateApp(app), c.IsNil)
	debugf(t, "created app %s (%s)", app.Name, app.ID)

	taffyRelease, err := client.GetAppRelease("taffy")
	t.Assert(err, c.IsNil)

	rwc, err := client.RunJobAttached("taffy", &ct.NewJob{
		ReleaseID: taffyRelease.ID,
		Cmd: []string{
			app.Name,
			github["clone_url"],
			github["ref"],
			github["sha"],
		},
		Meta: map[string]string{
			"type":       "github",
			"user_login": github["user_login"],
			"repo_name":  github["repo_name"],
			"ref":        github["ref"],
			"sha":        github["sha"],
			"clone_url":  github["clone_url"],
			"app":        app.ID,
		},
	})
	t.Assert(err, c.IsNil)
	attachClient := cluster.NewAttachClient(rwc)
	var outBuf bytes.Buffer
	exit, err := attachClient.Receive(&outBuf, &outBuf)
	t.Log(outBuf.String())
	outBuf.Reset()
	t.Assert(exit, c.Equals, 0)
	t.Assert(err, c.IsNil)

	// second deploy

	github["sha"] = "2bc7e016b1b4aae89396c898583763c5781e031a"

	artifact := &ct.Artifact{Type: "docker", URI: imageURIs["test-apps"]}
	t.Assert(client.CreateArtifact(artifact), c.IsNil)

	release, err := client.GetAppRelease(app.ID)
	t.Assert(err, c.IsNil)

	release = &ct.Release{
		ArtifactID: artifact.ID,
		Env:        release.Env,
		Processes:  release.Processes,
	}
	t.Assert(client.CreateRelease(release), c.IsNil)
	t.Assert(client.SetAppRelease(app.ID, release.ID), c.IsNil)

	taffyRelease, err = client.GetAppRelease("taffy")
	t.Assert(err, c.IsNil)

	rwc, err = client.RunJobAttached("taffy", &ct.NewJob{
		ReleaseID: taffyRelease.ID,
		Cmd: []string{
			app.Name,
			github["clone_url"],
			github["ref"],
			github["sha"],
		},
		Meta: map[string]string{
			"type":       "github",
			"user_login": github["user_login"],
			"repo_name":  github["repo_name"],
			"ref":        github["ref"],
			"sha":        github["sha"],
			"clone_url":  github["clone_url"],
			"app":        app.ID,
		},
	})
	t.Assert(err, c.IsNil)

	attachClient = cluster.NewAttachClient(rwc)
	exit, err = attachClient.Receive(&outBuf, &outBuf)
	t.Log(outBuf.String())
	t.Assert(exit, c.Equals, 0)
	t.Assert(err, c.IsNil)
}
