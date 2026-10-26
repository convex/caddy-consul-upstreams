package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/hashicorp/consul/api"
)

type consulFunc func(context.Context, string) ([]*api.ServiceEntry, error)

func (f consulFunc) Service(ctx context.Context, service string) ([]*api.ServiceEntry, error) {
	return f(ctx, service)
}

func cacheModule(t *testing.T, client consulClient) *ConsulUpstreams {
	t.Helper()
	u := testModule(nil)
	u.client = client
	t.Cleanup(func() {
		if err := u.Cleanup(); err != nil {
			t.Error(err)
		}
	})
	return u
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cache operation")
	}
}

func expireEntry(u *ConsulUpstreams, service string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.entries[service].freshness = time.Time{}
}

func waitFill(t *testing.T, u *ConsulUpstreams, service string) {
	t.Helper()
	u.mu.Lock()
	fill := u.entries[service].inflight
	u.mu.Unlock()
	if fill != nil {
		waitSignal(t, fill.done)
	}
}

func TestFreshServiceDoesNotWaitForUnrelatedFill(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	u := cacheModule(t, consulFunc(func(ctx context.Context, service string) ([]*api.ServiceEntry, error) {
		if service == "slow" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil
	}))
	if _, err := u.GetUpstreams(request("fast")); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_, _ = u.GetUpstreams(request("slow"))
		close(done)
	}()
	waitSignal(t, started)
	fast := make(chan struct{})
	go func() {
		defer close(fast)
		if _, err := u.GetUpstreams(request("fast")); err != nil {
			t.Error(err)
		}
	}()
	// The blocked slow call remains blocked until after the fast hit completes.
	waitSignal(t, fast)
	close(release)
	waitSignal(t, done)
}

func TestFailedFillsAreSharedAndBackedOff(t *testing.T) {
	var calls atomic.Int32
	var healthy atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		if calls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if !healthy.Load() {
			return nil, errors.New("Consul is down")
		}
		return nil, nil
	}))
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := u.GetUpstreams(request("api")); err == nil {
				t.Error("failed fill returned no error")
			}
		}()
	}
	waitSignal(t, started)
	close(release)
	group.Wait()
	for range 20 {
		_ = u.ResetCache(request("api"))
		if _, err := u.GetUpstreams(request("api")); err == nil {
			t.Fatal("failure backoff returned no error")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent failures and retries made %d calls, want 1", calls.Load())
	}
	healthy.Store(true)
	u.mu.Lock()
	u.entries["api"].nextTry = time.Time{}
	u.mu.Unlock()
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if got, err := u.GetUpstreams(request("api")); err != nil || len(got) != 0 {
				t.Errorf("recovery: %v %v", got, err)
			}
		}()
	}
	group.Wait()
	if calls.Load() != 2 {
		t.Fatalf("recovery made %d total calls, want 2", calls.Load())
	}
}

func TestFailureBackoffIncreasesAndCaps(t *testing.T) {
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(context.Context, string) ([]*api.ServiceEntry, error) {
		calls.Add(1)
		return nil, errors.New("down")
	}))
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		before := time.Now()
		if _, err := u.GetUpstreams(request("api")); err == nil {
			t.Fatal("failure returned no error")
		}
		u.mu.Lock()
		entry := u.entries["api"]
		remaining := time.Until(entry.nextTry)
		if remaining > want || entry.nextTry.Before(before.Add(want)) {
			t.Errorf("backoff deadline %s after start; want %s", entry.nextTry.Sub(before), want)
		}
		entry.nextTry = time.Time{}
		u.mu.Unlock()
	}
	u.Refresh = caddy.Duration(20 * time.Millisecond)
	if got := u.failureBackoff(10); got != 20*time.Millisecond {
		t.Fatalf("refresh cap: %s", got)
	}
	if calls.Load() != 7 {
		t.Fatalf("got %d calls", calls.Load())
	}
}

func TestGraceUsesBackgroundRefreshAndAuthoritativeEmpty(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		if calls.Add(1) == 1 {
			return []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil
		}
		close(started)
		select {
		case <-release:
			return nil, nil // A successful empty snapshot must replace the old one.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	if _, err := u.GetUpstreams(request("api")); err != nil {
		t.Fatal(err)
	}
	expireEntry(u, "api")
	got, err := u.GetUpstreams(request("api"))
	if err != nil || len(got) != 1 {
		t.Fatalf("stale result: %v %v", got, err)
	}
	waitSignal(t, started)
	close(release)
	waitFill(t, u, "api")
	got, err = u.GetUpstreams(request("api"))
	if err != nil || len(got) != 0 {
		t.Fatalf("empty successful result was not authoritative: %v %v", got, err)
	}
}

func TestGraceFailureKeepsStaleAndPreservesRetryDeadline(t *testing.T) {
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(context.Context, string) ([]*api.ServiceEntry, error) {
		if calls.Add(1) == 1 {
			return []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil
		}
		return nil, errors.New("down")
	}))
	if _, err := u.GetUpstreams(request("api")); err != nil {
		t.Fatal(err)
	}
	for failure := 0; failure < 3; failure++ {
		u.mu.Lock()
		u.entries["api"].freshness = time.Time{}
		u.entries["api"].nextTry = time.Time{}
		u.entries["api"].lastSuccess = time.Time{}
		u.mu.Unlock()
		if got, err := u.GetUpstreams(request("api")); err != nil || len(got) != 1 {
			t.Fatalf("stale stopped after failure %d: %v %v", failure, got, err)
		}
		waitFill(t, u, "api")
		u.mu.Lock()
		deadline := u.entries["api"].nextTry
		u.mu.Unlock()
		for range 10 {
			_ = u.ResetCache(request("api"))
			if got, err := u.GetUpstreams(request("api")); err != nil || len(got) != 1 {
				t.Fatalf("stale retry: %v %v", got, err)
			}
		}
		u.mu.Lock()
		if !deadline.Equal(u.entries["api"].nextTry) {
			t.Error("retry changed the grace retry deadline")
		}
		u.mu.Unlock()
		if calls.Load() != int32(failure+2) {
			t.Fatalf("retry bypassed grace deadline: %d calls", calls.Load())
		}
	}
}

func TestZeroGraceWaitsAndNeverReturnsStaleOnFailure(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		if calls.Add(1) == 1 {
			return []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil
		}
		close(started)
		select {
		case <-release:
			return nil, errors.New("down")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	u.GracePeriod = 0
	if _, err := u.GetUpstreams(request("api")); err != nil {
		t.Fatal(err)
	}
	expireEntry(u, "api")
	done := make(chan struct{})
	go func() {
		defer close(done)
		if got, err := u.GetUpstreams(request("api")); err == nil || got != nil {
			t.Errorf("zero grace served stale: %v %v", got, err)
		}
	}()
	waitSignal(t, started)
	select {
	case <-done:
		t.Fatal("zero grace returned before the refresh completed")
	default:
	}
	close(release)
	waitSignal(t, done)
	_ = u.ResetCache(request("api"))
	if _, err := u.GetUpstreams(request("api")); err == nil || calls.Load() != 2 {
		t.Fatalf("stale zero-grace failure backoff: err=%v calls=%d", err, calls.Load())
	}
}

func TestResetThrottlesSuccessfulFills(t *testing.T) {
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(context.Context, string) ([]*api.ServiceEntry, error) {
		calls.Add(1)
		return nil, nil
	}))
	u.GracePeriod = 0
	r := request("api")
	for range 50 {
		_ = u.ResetCache(r)
		if _, err := u.GetUpstreams(r); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("retry loop made %d calls", calls.Load())
	}
	u.mu.Lock()
	u.entries["api"].lastSuccess = time.Now().Add(-2 * resetMinInterval)
	u.mu.Unlock()
	_ = u.ResetCache(r)
	if _, err := u.GetUpstreams(r); err != nil || calls.Load() != 2 {
		t.Fatalf("reset after throttle: %v, calls %d", err, calls.Load())
	}
}

func TestCallerCancellationDoesNotCancelSharedFill(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return []*api.ServiceEntry{instance("10.0.0.1", "", 80, nil)}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	r := request("api")
	ctx, cancel := context.WithCancel(r.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := u.GetUpstreams(r.WithContext(ctx)); !errors.Is(err, context.Canceled) {
			t.Errorf("canceled caller: %v", err)
		}
	}()
	waitSignal(t, started)
	cancel()
	waitSignal(t, done)
	close(release)
	waitFill(t, u, "api")
	if got, err := u.GetUpstreams(request("api")); err != nil || len(got) != 1 || calls.Load() != 1 {
		t.Fatalf("shared fill did not survive caller: %v %v calls=%d", got, err, calls.Load())
	}
}

func TestCacheEvictsLeastRecentlyUsedCompletedEntry(t *testing.T) {
	u := cacheModule(t, consulFunc(func(context.Context, string) ([]*api.ServiceEntry, error) { return nil, nil }))
	for i := range cacheLimit {
		if _, err := u.GetUpstreams(request(fmt.Sprintf("service-%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := u.GetUpstreams(request("service-000")); err != nil {
		t.Fatal(err)
	}
	if _, err := u.GetUpstreams(request("additional")); err != nil {
		t.Fatal(err)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.entries["service-000"] == nil || u.entries["service-001"] != nil || len(u.entries) != cacheLimit {
		t.Fatal("cache did not evict the least recently used completed entry")
	}
}

func TestCacheBoundsActiveFillsAndCapacity(t *testing.T) {
	started := make(chan struct{}, cacheLimit)
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	var group sync.WaitGroup
	for i := range cacheLimit {
		group.Add(1)
		go func() {
			defer group.Done()
			r := request(fmt.Sprintf("service-%d", i))
			_, _ = u.GetUpstreams(r)
		}()
	}
	for range fillLimit {
		waitSignal(t, started)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		u.mu.Lock()
		count := len(u.entries)
		u.mu.Unlock()
		if count == cacheLimit {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("requests did not reserve cache entries")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := u.GetUpstreams(request("additional")); err == nil || !strings.Contains(err.Error(), "full") {
		t.Fatalf("saturated cache: %v", err)
	}
	select {
	case <-started:
		t.Fatal("more than eight concurrent Consul calls")
	default:
	}
	if err := u.Cleanup(); err != nil {
		t.Fatal(err)
	}
	group.Wait()
}

func TestGlobalResetKeepsActiveFillCoalescedWithoutRepopulating(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	done := make(chan struct{})
	go func() {
		_, _ = u.GetUpstreams(request("api"))
		close(done)
	}()
	waitSignal(t, started)
	if err := u.ResetCache(nil); err != nil {
		t.Fatal(err)
	}
	u.mu.Lock()
	fill := u.entries["api"].inflight
	if !u.entries["api"].discard || fill == nil {
		t.Error("global reset lost active fill bookkeeping")
	}
	u.mu.Unlock()
	close(release)
	waitSignal(t, done)
	u.mu.Lock()
	_, exists := u.entries["api"]
	u.mu.Unlock()
	if exists {
		t.Fatal("old fill repopulated a globally reset cache")
	}
	if _, err := u.GetUpstreams(request("api")); err != nil || calls.Load() != 2 {
		t.Fatalf("lookup after global reset: %v, calls %d", err, calls.Load())
	}
}

func TestCleanupCancelsFillsAndRejectsNewWork(t *testing.T) {
	started := make(chan struct{})
	u := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	done := make(chan struct{})
	go func() {
		_, _ = u.GetUpstreams(request("api"))
		close(done)
	}()
	waitSignal(t, started)
	if err := u.Cleanup(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, done)
	if _, err := u.GetUpstreams(request("other")); !errors.Is(err, context.Canceled) {
		t.Fatalf("work after cleanup: %v", err)
	}
}

func TestCleanupClosesConsulIdleConnections(t *testing.T) {
	idle, closed := make(chan struct{}, 1), make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[]`))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateIdle {
			idle <- struct{}{}
		}
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	u := &ConsulUpstreams{Service: "api", Address: server.URL}
	if err := u.Provision(caddy.Context{Context: context.Background()}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Cleanup() })
	if _, err := u.GetUpstreams(request("api")); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, idle)
	if err := u.Cleanup(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, closed)
}

func TestTimeoutOptionAndSharedFillDeadline(t *testing.T) {
	for _, config := range []string{"consul {\n service api\n timeout -1s\n}", "consul {\n service api\n timeout invalid\n}"} {
		if err := new(ConsulUpstreams).UnmarshalCaddyfile(caddyfile.NewTestDispenser(config)); err == nil {
			t.Fatalf("accepted invalid timeout: %s", config)
		}
	}
	u := new(ConsulUpstreams)
	if err := u.UnmarshalCaddyfile(caddyfile.NewTestDispenser("consul {\n service api\n timeout 20ms\n}")); err != nil {
		t.Fatal(err)
	}
	if u.Timeout != caddy.Duration(20*time.Millisecond) {
		t.Fatalf("configured timeout: %s", time.Duration(u.Timeout))
	}
	module := cacheModule(t, consulFunc(func(ctx context.Context, _ string) ([]*api.ServiceEntry, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	module.Timeout = u.Timeout
	if _, err := module.GetUpstreams(request("api")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared call did not use timeout: %v", err)
	}
}

func TestModuleCancellationReleasesWaiterBeforeUncooperativeClient(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	u := cacheModule(t, consulFunc(func(context.Context, string) ([]*api.ServiceEntry, error) {
		close(started)
		<-release // Exercise a custom client that does not honor cancellation.
		return nil, nil
	}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := u.GetUpstreams(request("api")); !errors.Is(err, context.Canceled) {
			t.Errorf("module cancellation: %v", err)
		}
	}()
	waitSignal(t, started)
	u.cancel()
	waitSignal(t, done)
	close(release)
	waitFill(t, u, "api")
}
