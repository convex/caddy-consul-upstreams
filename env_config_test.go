package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/hashicorp/consul/api"
)

func TestProvisionExpandsStaticEnvironmentFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/v1/health/service/api" {
			t.Errorf("expanded address used unexpected path %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode([]*api.ServiceEntry{
			instance("10.0.0.1", "", 8080, map[string]string{"az": "a"}),
			instance("10.0.0.2", "", 8080, map[string]string{"az": "b"}),
		})
	}))
	defer server.Close()
	t.Setenv("TEST_CONSUL_ADDR", server.URL+"/prefix")
	t.Setenv("TEST_CONSUL_META", "az")
	t.Setenv("TEST_CADDY_AZ", "a")
	var u ConsulUpstreams
	if err := json.Unmarshal([]byte(`{
		"service": "api",
		"address": "{env.TEST_CONSUL_ADDR}",
		"locality": {
			"node_meta": "{env.TEST_CONSUL_META}",
			"local_value": "{env.TEST_CADDY_AZ}",
			"minimum": 1
		}
	}`), &u); err != nil {
		t.Fatal(err)
	}
	if err := u.Provision(caddy.Context{Context: context.Background()}); err != nil {
		t.Fatal(err)
	}
	if u.Address != server.URL+"/prefix" || u.Locality.NodeMetaKey != "az" || u.Locality.LocalValue != "a" {
		t.Fatalf("unexpected expanded configuration: address=%q, locality=%+v", u.Address, u.Locality)
	}
	r := request("unused")
	got, err := u.GetUpstreams(r)
	if err != nil || len(got) != 1 || got[0].Dial != "10.0.0.1:8080" || tier(r) != "local" {
		t.Fatalf("expanded configuration discovery: upstreams=%v, error=%v, tier=%q", got, err, tier(r))
	}
}

func TestProvisionValidatesExpandedLocality(t *testing.T) {
	t.Setenv("TEST_EMPTY_CONSUL_META", "")
	t.Setenv("TEST_EMPTY_CADDY_AZ", "")
	for _, locality := range []*Locality{
		{NodeMetaKey: "az", LocalValue: "{env.TEST_EMPTY_CADDY_AZ}"},
		{NodeMetaKey: "{env.TEST_EMPTY_CONSUL_META}", LocalValue: "a"},
	} {
		t.Run(fmt.Sprintf("%s/%s", locality.NodeMetaKey, locality.LocalValue), func(t *testing.T) {
			f := &fakeConsul{}
			u := &ConsulUpstreams{Service: "api", Locality: locality, client: f}
			if err := u.Provision(caddy.Context{Context: context.Background()}); err == nil {
				t.Fatal("empty expanded locality field passed validation")
			}
			if f.callCount() != 0 {
				t.Fatal("invalid configuration queried Consul")
			}
		})
	}
}

type environmentServiceRecorder struct {
	mu       sync.Mutex
	services []string
}

func (f *environmentServiceRecorder) Service(_ context.Context, service string) ([]*api.ServiceEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.services = append(f.services, service)
	return nil, nil
}

func TestProvisionLeavesServiceExpansionForRequests(t *testing.T) {
	t.Setenv("TEST_CONSUL_SERVICE", "before-provision")
	f := new(environmentServiceRecorder)
	u := &ConsulUpstreams{Service: "{env.TEST_CONSUL_SERVICE}", client: f}
	if err := u.Provision(caddy.Context{Context: context.Background()}); err != nil {
		t.Fatal(err)
	}
	if u.Service != "{env.TEST_CONSUL_SERVICE}" {
		t.Fatalf("service expanded at provision time: %q", u.Service)
	}
	t.Setenv("TEST_CONSUL_SERVICE", "after-provision")
	if _, err := u.GetUpstreams(request("unused")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.services) != 1 || f.services[0] != "after-provision" {
		t.Fatalf("service was not expanded at request time: %v", f.services)
	}
}
