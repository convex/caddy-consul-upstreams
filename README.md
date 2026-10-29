# Caddy Consul upstreams

`caddy-consul-upstreams` is a Caddy v2 dynamic-upstream module that discovers
passing Consul service instances and can prefer instances in the proxy's zone.
It provides service discovery; it does not generate routes or configure Consul.

## Install

Build Caddy with [xcaddy](https://github.com/caddyserver/xcaddy):

```sh
xcaddy build v2.11.7 --with github.com/convex/caddy-consul-upstreams@v0.2.0
```

For local development, replace `@v0.2.0` with `=.`.

v0.2.0 is compatible with Caddy 2.11.7 and requires Go 1.26.9 or newer. Releases
before v0.2.0 were published under `github.com/convex/caddy-consul-upstreams`.

## Caddyfile

This example proxies to the `api` service and prefers instances with matching
node metadata. Set `CADDY_AZ` to this proxy's availability-zone value before
loading the Caddyfile, or replace `{$CADDY_AZ}` with a literal value.

```caddyfile
example.com {
	header >X-Consul-Tier {http.reverse_proxy.upstreams.consul.tier}
	reverse_proxy {
		dynamic consul {
			address http://127.0.0.1:8500
			service api
			refresh 10s
			grace_period 30s
			timeout 5s
			locality {
				node_meta availability-zone-id
				local_value {$CADDY_AZ}
				minimum 2
			}
		}
		lb_retries 2
		fail_duration 30s
	}
}
```

Omit `locality` to use all passing instances. The defaults are
`address http://127.0.0.1:8500`, `refresh 1m`, `grace_period 0s`, `timeout 5s`,
and locality `minimum 2`. `timeout` bounds each Consul request.

`lb_retries` enables retries, which Caddy disables by default. `fail_duration`
enables passive health checks so Caddy can avoid endpoints that recently
failed. Active health checks do not run for dynamic upstreams. With retries
enabled, Caddy can retry dial failures and the absence of an available upstream
before a request is sent. After a connection is established, transport-error
retries default to GET requests; use `lb_retry_match` carefully for other
methods, taking request replay and backend side effects into account.

## Service names and authorization

`service` accepts request placeholders. A service name must be 1–128 ASCII
letters, digits, hyphens, underscores, or periods, and cannot be `.` or `..`.
The length limit is a module limit. Invalid expanded names are rejected before
any Consul lookup.

Name validation checks syntax, not authorization. If a placeholder comes
directly from a request header, host, or path, the client can choose any service
visible to the module's Consul token. Use an explicit allowlist and a narrowly
scoped Consul ACL. Each distinct expanded name has its own lookup and cache
entry; the 100-entry cache bounds stored entries, not the number of possible
lookups. Unbounded request-controlled names can put load on Consul.

For example, Caddy's `map` directive can define a custom placeholder from known
hostnames. The empty default rejects unknown hosts instead of selecting a
service:

```caddyfile
:8080 {
	map {http.request.host} {consul_service} {
		api.example.test api
		web.example.test web
		default ""
	}
	header >X-Consul-Tier {http.reverse_proxy.upstreams.consul.tier}
	reverse_proxy {
		dynamic consul {
			address http://127.0.0.1:8500
			service {consul_service}
			refresh 10s
			grace_period 30s
		}
		lb_retries 2
		fail_duration 30s
	}
}
```

Here `{consul_service}` exists because `map` defines it. An undefined placeholder
expands to an empty service name and fails discovery. Without static fallback
upstreams, an unknown host in this example receives 503.

Scope the Consul token to the intended services as well. For the example above,
service-read grants for `api` and `web` avoid granting access to every service:

```hcl
service "api" {
  policy = "read"
}
service "web" {
  policy = "read"
}
node_prefix "" {
  policy = "read"
}
```

The node-read grant lets the health endpoint return the nodes hosting those
services. Adjust these grants to your Consul deployment and default ACL policy.

## JSON

The dynamic source object inside a reverse proxy's `dynamic_upstreams` field is:

```json
{
  "source": "consul",
  "address": "http://127.0.0.1:8500",
  "service": "api",
  "refresh": 10000000000,
  "grace_period": 30000000000,
  "timeout": 5000000000,
  "locality": {
    "node_meta": "availability-zone-id",
    "local_value": "a",
    "minimum": 2
  }
}
```

Numeric JSON durations are nanoseconds; duration strings such as `"10s"` are
also accepted. `{env.*}` placeholders in `address`,
`locality.node_meta`, and `locality.local_value` are expanded during provisioning.
The service string is expanded per request. Caddyfile `{$VAR}` placeholders are
expanded when the Caddyfile is adapted.

## Connecting to Consul

The client uses the Consul SDK's default configuration, including these
environment variables:

- `CONSUL_HTTP_TOKEN`, `CONSUL_HTTP_TOKEN_FILE`, and `CONSUL_HTTP_AUTH` for ACL
  tokens and HTTP basic authentication.
- `CONSUL_CACERT`, `CONSUL_CAPATH`, `CONSUL_CLIENT_CERT`, `CONSUL_CLIENT_KEY`,
  `CONSUL_TLS_SERVER_NAME`, and `CONSUL_HTTP_SSL_VERIFY` for TLS configuration.
- `CONSUL_NAMESPACE` and `CONSUL_PARTITION` for Enterprise scoping.

The module's `address` always takes precedence over `CONSUL_HTTP_ADDR`, including
its default `http://127.0.0.1:8500`. An explicit `http://` address forces plaintext
even when `CONSUL_HTTP_SSL=true`; use `https://` for TLS. For a bare host-and-port
address, the SDK's `CONSUL_HTTP_SSL` setting determines the scheme. Leave TLS
verification enabled in production. Unix socket addresses such as
`unix:///run/consul/http.sock` and reverse proxy path prefixes such as
`https://consul.example/prefix` are supported.

`CONSUL_HTTP_TOKEN_FILE` is read when the client is provisioned. Rotating that
file takes effect when the client is recreated, such as on a config reload that
reprovisions the module. A reload of unchanged configuration may be a no-op;
use `caddy reload --force` when needed.

## Cache and health behavior

The module queries Consul's `/v1/health/service/<name>?passing=1` endpoint.
Instances with any warning or critical check are excluded. A successful empty
response is authoritative and replaces the previous snapshot. For comparison,
Consul DNS excludes critical instances by default, but can include warning
instances unless `only_passing` is configured.

Snapshots are cached by expanded service name for `refresh`, with at most 100
entries. Concurrent first requests for the same service share a fill; fills for
different services can run concurrently, with at most eight Consul lookups
running at once. If all 100 entries have fills in flight, discovery for a new
service fails until capacity becomes available.
A disconnected client does not cancel a shared fill. Failed fills use backoff
so repeated requests do not repeatedly query Consul during an outage.

With `grace_period 0` (the default), expired snapshots are never served. Requests
wait for the refresh, and a failed refresh returns an error. Caddy then uses any
configured static fallback upstreams, or returns an error if none are available.

Set a positive `grace_period` to keep proxying while Consul is temporarily
unavailable. Expired snapshots are served while refreshing in the background.
After a failed refresh, the last snapshot continues to be served, with another
attempt after `grace_period`. This can continue indefinitely while Consul stays
down; the grace period is the retry interval, not a maximum snapshot age.
A service removed from Consul can therefore remain in use during an outage.

Retries can invalidate discovery, but refreshes are coalesced and throttled so
one failing backend does not cause a Consul lookup on every proxy attempt.

## Locality and observability

`locality.node_meta` names the metadata field on discovered nodes, and
`locality.local_value` is the required value for this Caddy instance. Missing
node metadata is nonlocal. An address registered more than once is local if any
of its registrations is local; addresses are deduplicated before counting them
against `minimum`.

For example, with local value `a` and passing instances in `a`, `a`, `b`, and `c`,
`minimum 2` selects the two `a` instances. If fewer than two distinct local
addresses are passing, all passing instances are used.

Locality is a preference: after a local attempt fails, a retry can use all
passing instances. This requires `lb_retries` or `lb_try_duration` to enable
retries. Add the `strict` flag inside `locality` (or `"strict": true` in JSON) to
keep the locality rule on retries even if local instances are unreachable.
Consul passing health and reachability from this proxy can differ.

The request placeholder `{http.reverse_proxy.upstreams.consul.tier}` is `local`,
`all`, or `fallback`. `fallback` means a retry widened a local pool to all
passing instances. It is available after discovery has run. Use a deferred
response header, as in the examples above; an ordinary non-deferred `header`
runs too early to read it. `header_up` inside `reverse_proxy` can also use it.
To add it to Caddy's access logs:

```caddyfile
log
log_append consul_tier {http.reverse_proxy.upstreams.consul.tier}
```

The module logs failed refreshes and debug-level refresh details. Enable Caddy's
`debug` global option when diagnosing discovery or locality decisions.

## Development

Run `go test -race ./...` and `go vet ./...`. CI also builds a real Caddy binary,
adapts both example Caddyfiles, and runs a local fake-Consul smoke test. To run
that smoke test against your own build:

```sh
python3 testdata/smoke.py --caddy ./caddy
```

## Scope

v0.2.0 intentionally has no tag locality, latency ordering, weights, WAN
federation, arbitrary preference expressions, or custom metrics.
