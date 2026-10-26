// Package consul provides a Caddy dynamic upstream source backed by Consul's
// passing service health endpoint.
package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

const (
	moduleID         = "http.reverse_proxy.upstreams.consul"
	tierPlaceholder  = "http.reverse_proxy.upstreams.consul.tier"
	consulTimeout    = 5 * time.Second
	cacheLimit       = 100
	fillLimit        = 8
	resetMinInterval = time.Second
	retryVar         = "consul_upstreams_selection"
)

// ConsulUpstreams discovers passing service instances through Consul.
type ConsulUpstreams struct {
	Address     string         `json:"address,omitempty"`
	Service     string         `json:"service,omitempty"`
	Refresh     caddy.Duration `json:"refresh,omitempty"`
	GracePeriod caddy.Duration `json:"grace_period,omitempty"`
	Timeout     caddy.Duration `json:"timeout,omitempty"`
	Locality    *Locality      `json:"locality,omitempty"`

	client    consulClient
	logger    *zap.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	closeIdle func()
	mu        sync.Mutex
	entries   map[string]*cacheEntry
	fills     chan struct{}
	wg        sync.WaitGroup
	closed    bool
}

// Locality controls local-node preference.
type Locality struct {
	NodeMetaKey string `json:"node_meta,omitempty"`
	LocalValue  string `json:"local_value,omitempty"`
	Minimum     int    `json:"minimum,omitempty"`
	Strict      bool   `json:"strict,omitempty"`
}

type consulClient interface {
	Service(context.Context, string) ([]*api.ServiceEntry, error)
}

type apiClient struct{ c *api.Client }

func (a apiClient) Service(ctx context.Context, service string) ([]*api.ServiceEntry, error) {
	entries, _, err := a.c.Health().Service(service, "", true, (&api.QueryOptions{}).WithContext(ctx))
	return entries, err
}

// cacheEntry fields are guarded by mu. Snapshots are immutable after creation.
type cacheEntry struct {
	snapshot    snapshot
	hasResult   bool
	freshness   time.Time
	lastSuccess time.Time
	lastUsed    time.Time
	nextTry     time.Time
	failures    int
	err         error
	inflight    *cacheFill
	discard     bool
}

// A waiter reads its own completed fill, even if a later fill has already started.
type cacheFill struct {
	done     chan struct{}
	snapshot snapshot
	err      error
}

type snapshot struct{ all, local []string }

func init() { caddy.RegisterModule(&ConsulUpstreams{}) }

func (*ConsulUpstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: moduleID, New: func() caddy.Module { return new(ConsulUpstreams) }}
}

func (u *ConsulUpstreams) Provision(ctx caddy.Context) error {
	repl := caddy.NewReplacer()
	address := repl.ReplaceAll(u.Address, "")
	if u.Address != "" && strings.TrimSpace(address) == "" {
		return errors.New("Consul address expanded to an empty value")
	}
	u.Address = address
	if u.Locality != nil {
		u.Locality.NodeMetaKey = repl.ReplaceAll(u.Locality.NodeMetaKey, "")
		u.Locality.LocalValue = repl.ReplaceAll(u.Locality.LocalValue, "")
	}
	if err := u.Validate(); err != nil {
		return err
	}
	u.logger = ctx.Logger()
	u.initCache(ctx.Context)
	if u.client == nil {
		client, closeIdle, err := newConsulClientWithCleanup(u.Address, time.Duration(u.Timeout))
		if err != nil {
			u.cancel()
			return fmt.Errorf("creating Consul client: %w", err)
		}
		u.client = apiClient{client}
		u.closeIdle = closeIdle
	}
	return nil
}

func (u *ConsulUpstreams) initCache(ctx context.Context) {
	u.ctx, u.cancel = context.WithCancel(ctx)
	u.entries = make(map[string]*cacheEntry)
	u.fills = make(chan struct{}, fillLimit)
}

func newConsulClient(address string, timeout time.Duration) (*api.Client, error) {
	client, _, err := newConsulClientWithCleanup(address, timeout)
	return client, err
}

func newConsulClientWithCleanup(address string, timeout time.Duration) (*api.Client, func(), error) {
	config := api.DefaultConfig()
	config.Address = address
	if strings.HasPrefix(address, "http://") {
		config.Scheme = "http"
	} else if strings.HasPrefix(address, "https://") {
		config.Scheme = "https"
	}
	client, err := api.NewClient(config)
	if err != nil {
		return nil, nil, err
	}
	config.HttpClient.Timeout = timeout
	return client, config.HttpClient.CloseIdleConnections, nil
}

func (u *ConsulUpstreams) Validate() error {
	if u.Address == "" {
		u.Address = "http://127.0.0.1:8500"
	}
	if u.Refresh == 0 {
		u.Refresh = caddy.Duration(time.Minute)
	}
	if u.Timeout == 0 {
		u.Timeout = caddy.Duration(consulTimeout)
	}
	if u.Timeout < 0 {
		return errors.New("timeout must be positive")
	}
	if strings.TrimSpace(u.Service) == "" {
		return errors.New("service is required")
	}
	if !strings.Contains(u.Service, "{") && !validServiceName(strings.TrimSpace(u.Service)) {
		return fmt.Errorf("invalid Consul service name %q", u.Service)
	}
	if u.Refresh < 0 || u.GracePeriod < 0 {
		return errors.New("refresh and grace_period must not be negative")
	}
	if u.Locality != nil {
		if strings.TrimSpace(u.Locality.NodeMetaKey) == "" {
			return errors.New("locality.node_meta is required")
		}
		if strings.TrimSpace(u.Locality.LocalValue) == "" {
			return errors.New("locality.local_value is required")
		}
		if u.Locality.Minimum == 0 {
			u.Locality.Minimum = 2
		}
		if u.Locality.Minimum < 1 {
			return errors.New("locality.minimum must be at least one")
		}
	}
	return nil
}

func (u *ConsulUpstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	service := u.expandedService(r)
	if !validServiceName(service) {
		return nil, fmt.Errorf("invalid Consul service name %q", service)
	}

	ctx := context.Background()
	if r != nil {
		ctx = r.Context()
	}
	snapshot, err := u.getSnapshot(ctx, service)
	if err != nil {
		return nil, err
	}
	tier, dials := u.selectForRequest(r, service, snapshot)
	setTier(r, tier)
	return upstreams(dials), nil
}

func (u *ConsulUpstreams) getSnapshot(ctx context.Context, service string) (snapshot, error) {
	if err := ctx.Err(); err != nil {
		return snapshot{}, err
	}
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return snapshot{}, context.Canceled
	}
	if err := u.ctx.Err(); err != nil {
		u.mu.Unlock()
		return snapshot{}, err
	}
	entry, ok := u.entries[service]
	if !ok {
		if len(u.entries) >= cacheLimit && !u.evictLocked() {
			u.mu.Unlock()
			return snapshot{}, errors.New("Consul upstream cache is full with active refreshes")
		}
		entry = new(cacheEntry)
		u.entries[service] = entry
	}
	now := time.Now()
	entry.lastUsed = now
	if entry.isFresh(u.Refresh) {
		result := entry.snapshot
		u.mu.Unlock()
		return result, nil
	}
	if entry.hasResult && u.GracePeriod > 0 {
		if entry.inflight == nil && !now.Before(entry.nextTry) {
			u.startFillLocked(service, entry)
		}
		result := entry.snapshot
		u.mu.Unlock()
		return result, nil
	}
	if entry.inflight == nil && now.Before(entry.nextTry) {
		err := entry.err
		u.mu.Unlock()
		return snapshot{}, err
	}
	if entry.inflight == nil {
		u.startFillLocked(service, entry)
	}
	fill := entry.inflight
	u.mu.Unlock()

	select {
	case <-ctx.Done():
		return snapshot{}, ctx.Err()
	case <-u.ctx.Done():
		return snapshot{}, u.ctx.Err()
	case <-fill.done:
		return fill.snapshot, fill.err
	}
}

// evictLocked keeps active fills in the cache so a new request cannot start a
// duplicate fill for the same service. If all entries are active, reject a new
// service rather than letting untrusted service names grow the cache or queue.
func (u *ConsulUpstreams) evictLocked() bool {
	var oldest string
	var oldestTime time.Time
	for key, entry := range u.entries {
		if entry.inflight == nil && (oldest == "" || entry.lastUsed.Before(oldestTime)) {
			oldest, oldestTime = key, entry.lastUsed
		}
	}
	if oldest == "" {
		return false
	}
	delete(u.entries, oldest)
	return true
}

// startFillLocked registers work before releasing mu, which also serializes
// WaitGroup.Add with Cleanup's transition to the closed state.
func (u *ConsulUpstreams) startFillLocked(service string, entry *cacheEntry) {
	fill := &cacheFill{done: make(chan struct{})}
	entry.inflight = fill
	u.wg.Add(1)
	go u.refresh(service, entry, fill)
}

func (u *ConsulUpstreams) refresh(service string, entry *cacheEntry, fill *cacheFill) {
	defer u.wg.Done()
	var result snapshot
	var err error
	start := time.Now()
	select {
	case u.fills <- struct{}{}:
		defer func() { <-u.fills }()
		if u.logger != nil {
			u.logger.Debug("refreshing Consul upstreams", zap.String("service", service))
		}
		ctx, cancel := context.WithTimeout(u.ctx, time.Duration(u.Timeout))
		entries, queryErr := u.client.Service(ctx, service)
		cancel()
		err = queryErr
		if err == nil {
			result = u.makeSnapshot(entries)
		}
	case <-u.ctx.Done():
		err = u.ctx.Err()
	}

	u.mu.Lock()
	now := time.Now()
	if err == nil {
		entry.snapshot, entry.hasResult = result, true
		entry.freshness, entry.lastSuccess = now, now
		entry.nextTry, entry.failures, entry.err = time.Time{}, 0, nil
		fill.snapshot = result
	} else {
		entry.err = fmt.Errorf("refreshing Consul service %q: %w", service, err)
		entry.failures++
		if entry.hasResult && u.GracePeriod > 0 {
			entry.freshness = now.Add(time.Duration(u.GracePeriod) - time.Duration(u.Refresh))
			entry.nextTry = now.Add(time.Duration(u.GracePeriod))
		} else {
			entry.nextTry = now.Add(u.failureBackoff(entry.failures))
		}
		fill.err = entry.err
	}
	servedStale := entry.hasResult && u.GracePeriod > 0
	entry.inflight = nil
	if entry.discard {
		delete(u.entries, service)
	}
	close(fill.done)
	u.mu.Unlock()

	if u.logger != nil {
		if err == nil {
			u.logger.Debug("refreshed Consul upstreams", zap.String("service", service), zap.Int("instances", len(result.all)), zap.Int("local", len(result.local)), zap.Duration("duration", time.Since(start)))
		} else {
			message := "Consul upstream refresh failed"
			if servedStale {
				message += "; serving cached snapshot"
			}
			u.logger.Warn(message, zap.String("service", service), zap.Error(err))
		}
	}
}

func (u *ConsulUpstreams) failureBackoff(failures int) time.Duration {
	limit := min(time.Duration(u.Refresh), 30*time.Second)
	backoff := min(time.Second, limit)
	for failure := 1; failure < failures && backoff < limit; failure++ {
		backoff = min(2*backoff, limit)
	}
	return backoff
}

func (entry *cacheEntry) isFresh(refresh caddy.Duration) bool {
	return entry.hasResult && time.Since(entry.freshness) < time.Duration(refresh)
}

// Cleanup stops shared work even when called directly, and waits only a bounded
// time if a custom client fails to honor cancellation.
func (u *ConsulUpstreams) Cleanup() error {
	u.mu.Lock()
	u.closed = true
	if u.cancel != nil {
		u.cancel()
	}
	u.mu.Unlock()
	if u.closeIdle != nil {
		defer u.closeIdle()
	}
	done := make(chan struct{})
	go func() {
		u.wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(min(time.Duration(u.Timeout), consulTimeout) + time.Second)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return errors.New("timed out stopping Consul upstream refreshes")
	}
}

func (u *ConsulUpstreams) selectTier(snapshot snapshot, retried bool) (string, []string) {
	if u.Locality == nil {
		return "all", snapshot.all
	}
	if len(snapshot.local) >= u.Locality.Minimum {
		if retried && !u.Locality.Strict {
			return "fallback", snapshot.all
		}
		return "local", snapshot.local
	}
	return "all", snapshot.all
}

// requestSelection is scoped to a source instance and service so retry state
// cannot affect another reverse_proxy handler sharing the request's variables.
type requestSelection struct {
	tier    string
	retried bool
}

func (u *ConsulUpstreams) selectionKey(service string) string {
	return fmt.Sprintf("%s.%p.%s", retryVar, u, service)
}

func (u *ConsulUpstreams) selectForRequest(r *http.Request, service string, snapshot snapshot) (string, []string) {
	var state requestSelection
	if r != nil {
		state, _ = caddyhttp.GetVar(r.Context(), u.selectionKey(service)).(requestSelection)
	}
	tier, dials := u.selectTier(snapshot, state.retried && (state.tier == "local" || state.tier == "fallback"))
	// A retry must not narrow an already-global pool if Consul's refreshed
	// snapshot happens to have enough local registrations this time.
	if state.retried && state.tier == "all" {
		tier, dials = "all", snapshot.all
	}
	if r != nil {
		state.tier = tier
		caddyhttp.SetVar(r.Context(), u.selectionKey(service), state)
	}
	return tier, dials
}

func (u *ConsulUpstreams) markRetry(r *http.Request, service string) {
	state, _ := caddyhttp.GetVar(r.Context(), u.selectionKey(service)).(requestSelection)
	if state.tier != "" {
		state.retried = true
		caddyhttp.SetVar(r.Context(), u.selectionKey(service), state)
	}
}

func (u *ConsulUpstreams) ResetCache(r *http.Request) error {
	var service string
	if r != nil {
		service = u.expandedService(r)
		if !validServiceName(service) {
			return nil
		}
		u.markRetry(r, service)
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if r == nil {
		for service, entry := range u.entries {
			if entry.inflight == nil {
				delete(u.entries, service)
			} else {
				// Retain active slots until they finish, then discard their results.
				entry.hasResult = false
				entry.discard = true
			}
		}
		return nil
	}
	if entry, ok := u.entries[service]; ok && time.Since(entry.lastSuccess) >= resetMinInterval {
		entry.freshness = time.Time{}
		// In-flight work and failure backoff survive request retries.
	}
	return nil
}

func (u *ConsulUpstreams) makeSnapshot(entries []*api.ServiceEntry) snapshot {
	type locality struct{ local, nonlocal bool }
	seen := make(map[string]*locality, len(entries))
	var result snapshot
	for _, entry := range entries {
		if entry == nil || entry.Node == nil || entry.Service == nil || entry.Service.Port < 1 || entry.Service.Port > 65535 {
			continue
		}
		host := entry.Service.Address
		if host == "" {
			host = entry.Node.Address
		}
		if host == "" {
			continue
		}
		dial := net.JoinHostPort(host, strconv.Itoa(entry.Service.Port))
		location, ok := seen[dial]
		if !ok {
			location = new(locality)
			seen[dial] = location
			result.all = append(result.all, dial)
		}
		if u.Locality != nil && entry.Node.Meta[u.Locality.NodeMetaKey] == u.Locality.LocalValue {
			location.local = true
		} else {
			location.nonlocal = true
		}
	}
	var conflicting []string
	for _, dial := range result.all {
		location := seen[dial]
		if location.local {
			result.local = append(result.local, dial)
			if location.nonlocal {
				conflicting = append(conflicting, dial)
			}
		}
	}
	if len(conflicting) > 0 && u.logger != nil {
		u.logger.Debug("Consul registrations have conflicting locality metadata", zap.Strings("upstreams", conflicting))
	}
	return result
}

func (u *ConsulUpstreams) expandedService(r *http.Request) string {
	if r == nil {
		return strings.TrimSpace(u.Service)
	}
	repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer)
	if !ok {
		return strings.TrimSpace(u.Service)
	}
	return strings.TrimSpace(repl.ReplaceAll(u.Service, ""))
}

func validServiceName(name string) bool {
	// Consul's HTTP API does not impose this limit; bound request-derived names
	// conservatively to keep cache keys and requests small.
	if len(name) == 0 || len(name) > 128 || name == "." || name == ".." {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
}

func upstreams(dials []string) []*reverseproxy.Upstream {
	result := make([]*reverseproxy.Upstream, len(dials))
	for index, dial := range dials {
		result[index] = &reverseproxy.Upstream{Dial: dial}
	}
	return result
}

func tier(r *http.Request) string {
	value, _ := caddyhttp.GetVar(r.Context(), tierPlaceholder).(string)
	return value
}

func setTier(r *http.Request, value string) {
	if r == nil {
		return
	}
	caddyhttp.SetVar(r.Context(), tierPlaceholder, value)
	if repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer); ok {
		repl.Set(tierPlaceholder, value)
	}
}

func (u *ConsulUpstreams) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	for d.NextBlock(0) {
		switch d.Val() {
		case "address", "service", "refresh", "grace_period", "timeout":
			option := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			value := d.Val()
			if d.NextArg() {
				return d.ArgErr()
			}
			switch option {
			case "address":
				u.Address = value
			case "service":
				u.Service = value
			default:
				duration, err := caddy.ParseDuration(value)
				if err != nil {
					return d.Errf("invalid %s: %v", option, err)
				}
				if option == "refresh" {
					u.Refresh = caddy.Duration(duration)
				} else if option == "grace_period" {
					u.GracePeriod = caddy.Duration(duration)
				} else {
					u.Timeout = caddy.Duration(duration)
				}
			}
		case "locality":
			if u.Locality != nil {
				return d.Err("locality is already specified")
			}
			u.Locality = &Locality{}
			for nesting := d.Nesting(); d.NextBlock(nesting); {
				switch d.Val() {
				case "strict":
					if d.NextArg() {
						return d.ArgErr()
					}
					u.Locality.Strict = true
				case "node_meta":
					if !d.NextArg() {
						return d.ArgErr()
					}
					u.Locality.NodeMetaKey = d.Val()
					if d.NextArg() {
						return d.ArgErr()
					}
				case "local_value":
					if !d.NextArg() {
						return d.ArgErr()
					}
					u.Locality.LocalValue = d.Val()
					if d.NextArg() {
						return d.ArgErr()
					}
				case "minimum":
					if !d.NextArg() {
						return d.ArgErr()
					}
					minimum, err := strconv.Atoi(d.Val())
					if err != nil {
						return d.Errf("invalid locality minimum: %v", err)
					}
					u.Locality.Minimum = minimum
					if d.NextArg() {
						return d.ArgErr()
					}
				default:
					return d.Errf("unrecognized locality option %q", d.Val())
				}
			}
		default:
			return d.Errf("unrecognized consul option %q", d.Val())
		}
	}
	return u.Validate()
}

var (
	_ caddy.Module                       = (*ConsulUpstreams)(nil)
	_ caddy.Provisioner                  = (*ConsulUpstreams)(nil)
	_ caddy.Validator                    = (*ConsulUpstreams)(nil)
	_ caddy.CleanerUpper                 = (*ConsulUpstreams)(nil)
	_ caddyfile.Unmarshaler              = (*ConsulUpstreams)(nil)
	_ reverseproxy.UpstreamSource        = (*ConsulUpstreams)(nil)
	_ reverseproxy.CachingUpstreamSource = (*ConsulUpstreams)(nil)
)
