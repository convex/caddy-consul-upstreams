package consul

import (
	"net/http"
	"testing"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/hashicorp/consul/api"
)

func retryLocalityModule() *ConsulUpstreams {
	u := testModule(&fakeConsul{entries: []*api.ServiceEntry{
		instance("10.0.0.1", "", 80, map[string]string{"az": "a"}),
		instance("10.0.0.2", "", 80, map[string]string{"az": "a"}),
		instance("10.0.1.1", "", 80, map[string]string{"az": "b"}),
	}})
	u.Locality = &Locality{NodeMetaKey: "az", LocalValue: "a", Minimum: 2}
	return u
}

func assertSelection(t *testing.T, u *ConsulUpstreams, r *http.Request, count int, wantTier string) {
	t.Helper()
	got, err := u.GetUpstreams(r)
	if err != nil || len(got) != count || tier(r) != wantTier {
		t.Fatalf("got count=%d tier=%q err=%v; want count=%d tier=%q", len(got), tier(r), err, count, wantTier)
	}
}

func TestLocalityFallsBackOnRetry(t *testing.T) {
	u := retryLocalityModule()
	r := request("api")
	assertSelection(t, u, r, 2, "local")
	if err := u.ResetCache(r); err != nil {
		t.Fatal(err)
	}
	assertSelection(t, u, r, 3, "fallback")
	if err := u.ResetCache(r); err != nil {
		t.Fatal(err)
	}
	assertSelection(t, u, r, 3, "fallback")
	assertSelection(t, u, request("api"), 2, "local")
}

func TestLocalityStrictRetry(t *testing.T) {
	u := retryLocalityModule()
	u.Locality.Strict = true
	r := request("api")
	assertSelection(t, u, r, 2, "local")
	_ = u.ResetCache(r)
	assertSelection(t, u, r, 2, "local")
}

func TestRetryAlreadyAll(t *testing.T) {
	for _, locality := range []*Locality{nil, {NodeMetaKey: "az", LocalValue: "a", Minimum: 4}} {
		u := retryLocalityModule()
		u.Locality = locality
		r := request("api")
		assertSelection(t, u, r, 3, "all")
		_ = u.ResetCache(r)
		assertSelection(t, u, r, 3, "all")
	}
}

func TestRetryStateIsScopedToSourceAndService(t *testing.T) {
	first, second := retryLocalityModule(), retryLocalityModule()
	r := request("api")
	assertSelection(t, first, r, 2, "local")
	_ = first.ResetCache(r)
	assertSelection(t, first, r, 3, "fallback")
	assertSelection(t, second, r, 2, "local")

	repl := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	repl.Set("service", "web")
	assertSelection(t, first, r, 2, "local")
	repl.Set("service", "api")
	assertSelection(t, first, r, 3, "fallback")
}

func TestRetryDoesNotNarrowGlobalSelection(t *testing.T) {
	u := retryLocalityModule()
	u.Locality.Minimum = 4
	r := request("api")
	assertSelection(t, u, r, 3, "all")
	u.Locality.Minimum = 2
	_ = u.ResetCache(r)
	assertSelection(t, u, r, 3, "all")
}

func TestResetBeforeSelectionDoesNotCauseFallback(t *testing.T) {
	u := retryLocalityModule()
	r := request("api")
	_ = u.ResetCache(r)
	assertSelection(t, u, r, 2, "local")
}

func TestLocalityStrictCaddyfile(t *testing.T) {
	for _, tc := range []struct {
		name, option          string
		wantError, wantStrict bool
	}{
		{name: "default"},
		{name: "strict", option: "strict", wantStrict: true},
		{name: "argument", option: "strict off", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := new(ConsulUpstreams)
			err := u.UnmarshalCaddyfile(caddyfile.NewTestDispenser("consul {\nservice api\nlocality {\nnode_meta az\nlocal_value a\n" + tc.option + "\n}\n}"))
			if (err != nil) != tc.wantError {
				t.Fatalf("error: %v", err)
			}
			if err == nil && u.Locality.Strict != tc.wantStrict {
				t.Fatalf("strict: got %v, want %v", u.Locality.Strict, tc.wantStrict)
			}
		})
	}
}
