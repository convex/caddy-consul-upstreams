package consul

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func TestServiceNameValidation(t *testing.T) {
	valid := []string{"api", "web-1", "a.b_c", "API", "...", strings.Repeat("a", 128)}
	invalid := []string{"../../agent/self", "a/b", "..", ".", "a b", "api?x=1", "api#x", "", strings.Repeat("a", 129), "café", "a\\b", "a\x00b", "%2e%2e"}
	for _, name := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			if !validServiceName(name) {
				t.Fatalf("rejected valid service name %q", name)
			}
			if err := (&ConsulUpstreams{Service: name}).Validate(); err != nil {
				t.Fatalf("static service validation failed: %v", err)
			}
		})
	}
	for _, name := range invalid {
		t.Run("invalid/"+name, func(t *testing.T) {
			if validServiceName(name) {
				t.Fatalf("accepted invalid service name %q", name)
			}
			if err := (&ConsulUpstreams{Service: name}).Validate(); err == nil {
				t.Fatal("accepted invalid static service configuration")
			}
			f := &fakeConsul{}
			u := testModule(f)
			if _, err := u.GetUpstreams(request(name)); err == nil {
				t.Fatal("accepted invalid expanded service")
			}
			if err := u.ResetCache(request(name)); err != nil {
				t.Fatalf("invalid-name cache reset should be a no-op: %v", err)
			}
			if f.callCount() != 0 || len(u.entries) != 0 {
				t.Fatal("invalid service reached Consul or the cache")
			}
		})
	}
	if err := (&ConsulUpstreams{Service: "{http.request.header.Service}"}).Validate(); err != nil {
		t.Fatalf("request placeholder rejected at configuration time: %v", err)
	}
}

func TestServiceTraversalNeverReachesConsul(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/health/service/api" {
			t.Errorf("unexpected Consul path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()
	client, err := newConsulClient(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	u := testModule(nil)
	u.client = apiClient{client}
	if _, err := u.GetUpstreams(request("../../agent/self")); err == nil || !strings.Contains(err.Error(), "invalid Consul service name") {
		t.Fatalf("expected service validation error, got %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("traversal issued a Consul request")
	}
	if _, err := u.GetUpstreams(request(" api ")); err != nil {
		t.Fatalf("valid expanded service failed: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("valid service made %d requests, want 1", calls.Load())
	}
}

func TestStaticServiceWhitespaceIsTrimmed(t *testing.T) {
	u := &ConsulUpstreams{Service: " api "}
	if err := u.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := u.expandedService(nil); got != "api" {
		t.Fatalf("nil request expansion = %q", got)
	}
	r, err := http.NewRequest(http.MethodGet, "http://example.test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := u.expandedService(r); got != "api" {
		t.Fatalf("request without a replacer expansion = %q", got)
	}
}

func TestInvalidResetPreservesCachedServices(t *testing.T) {
	for _, name := range []string{"../../agent/self", "", "..", strings.Repeat("a", 129)} {
		t.Run(name, func(t *testing.T) {
			f := &fakeConsul{entries: []*api.ServiceEntry{instance("10.0.0.1", "", 8080, nil)}}
			u := testModule(f)
			u.GracePeriod = 0
			if _, err := u.GetUpstreams(request("api")); err != nil {
				t.Fatal(err)
			}
			f.set([]*api.ServiceEntry{instance("10.0.0.2", "", 8080, nil)}, nil)
			if err := u.ResetCache(request(name)); err != nil {
				t.Fatal(err)
			}
			got, err := u.GetUpstreams(request("api"))
			if err != nil || len(got) != 1 || got[0].Dial != "10.0.0.1:8080" || f.callCount() != 1 {
				t.Fatalf("invalid reset changed a cached service: upstreams=%v error=%v calls=%d", got, err, f.callCount())
			}
		})
	}
}

func TestUnsafeServiceTemplatesNeverReachConsul(t *testing.T) {
	for _, template := range []string{"api{", "../../agent/self{unknown}", "{service}/admin"} {
		t.Run(template, func(t *testing.T) {
			f := &fakeConsul{}
			u := testModule(f)
			u.Service = template
			if err := u.Validate(); err != nil {
				t.Fatalf("dynamic templates are validated after expansion: %v", err)
			}
			if _, err := u.GetUpstreams(request("api")); err == nil || !strings.Contains(err.Error(), "invalid Consul service name") {
				t.Fatalf("expected service validation error, got %v", err)
			}
			if f.callCount() != 0 || len(u.entries) != 0 {
				t.Fatal("unsafe template reached Consul or the cache")
			}
		})
	}
}
