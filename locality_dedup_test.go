package consul

import (
	"reflect"
	"testing"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestDuplicateAddressLocalityIsOrderIndependent(t *testing.T) {
	local := instance("node-a", "shared.internal", 80, map[string]string{"az": "a"})
	remote := instance("node-b", "shared.internal", 80, map[string]string{"az": "b"})
	missing := instance("node-c", "shared.internal", 80, nil)
	localOnly := instance("10.0.0.2", "", 80, map[string]string{"az": "a"})
	remoteOnly := instance("10.0.1.2", "", 80, map[string]string{"az": "b"})
	for name, duplicates := range map[string][]*api.ServiceEntry{
		"local then remote":  {local, remote},
		"remote then local":  {remote, local},
		"missing then local": {missing, local},
		"local then missing": {local, missing},
	} {
		t.Run(name, func(t *testing.T) {
			core, logs := observer.New(zap.DebugLevel)
			u := &ConsulUpstreams{Locality: &Locality{NodeMetaKey: "az", LocalValue: "a", Minimum: 2}, logger: zap.New(core)}
			entries := append(append([]*api.ServiceEntry{}, duplicates...), localOnly, remoteOnly)
			got := u.makeSnapshot(entries)
			want := snapshot{all: []string{"shared.internal:80", "10.0.0.2:80", "10.0.1.2:80"}, local: []string{"shared.internal:80", "10.0.0.2:80"}}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("snapshot: got %#v, want %#v", got, want)
			}
			if tier, _ := u.selectTier(got, false); tier != "local" {
				t.Fatalf("tier: got %q, want local", tier)
			}
			if logs.FilterMessage("Consul registrations have conflicting locality metadata").Len() != 1 {
				t.Fatal("expected one conflict log per snapshot")
			}
		})
	}
}

func TestDuplicateAddressWithoutLocality(t *testing.T) {
	u := &ConsulUpstreams{}
	got := u.makeSnapshot([]*api.ServiceEntry{
		instance("node-a", "shared.internal", 80, map[string]string{"az": "a"}),
		instance("node-b", "shared.internal", 80, map[string]string{"az": "b"}),
	})
	if !reflect.DeepEqual(got.all, []string{"shared.internal:80"}) || len(got.local) != 0 {
		t.Fatalf("unexpected snapshot: %#v", got)
	}
}
