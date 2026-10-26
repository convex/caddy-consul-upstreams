#!/usr/bin/env python3
"""Exercise the built Caddy module against local fake Consul and HTTP servers."""

import argparse
import contextlib
import http.client
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit


class Fixtures:
    def __init__(self):
        self.lock = threading.Lock()
        self.entries = []
        self.consul_calls = 0
        self.consul_paths = []
        self.backend_calls = 0

    def counts(self):
        with self.lock:
            return self.consul_calls, self.backend_calls

    def use_entries(self, entries):
        with self.lock:
            self.entries = entries

    def request_paths(self):
        with self.lock:
            return list(self.consul_paths)


def consul_handler(fixtures):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlsplit(self.path)
            with fixtures.lock:
                fixtures.consul_calls += 1
                fixtures.consul_paths.append(url.path)
            if url.path not in ("/v1/health/service/api", "/v1/health/service/web"):
                self.send_error(404)
                return
            if parse_qs(url.query).get("passing") != ["1"]:
                self.send_error(400, "discovery must request passing instances")
                return
            with fixtures.lock:
                body = json.dumps(fixtures.entries).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


def backend_handler(fixtures):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            with fixtures.lock:
                fixtures.backend_calls += 1
            body = b"healthy remote backend\n"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


@contextlib.contextmanager
def server(handler):
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=lambda: httpd.serve_forever(poll_interval=0.05))
    thread.start()
    try:
        yield httpd.server_port
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def unused_socket():
    # Keep dead endpoint ports reserved without listening, so another process
    # cannot accidentally turn one of the "dead" backends into a working one.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    return sock


def instance(port, zone):
    return {
        "Node": {"Address": "127.0.0.1", "Meta": {"az": zone}},
        "Service": {"Address": "127.0.0.1", "Port": port},
    }


def get(port, host="example.test"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", "/", headers={"Host": host})
        response = conn.getresponse()
        return response.status, response.getheader("X-Consul-Tier"), response.read()
    finally:
        conn.close()


@contextlib.contextmanager
def caddy(binary, config, consul_port, workdir, label):
    with unused_socket() as reservation:
        port = reservation.getsockname()[1]
    env = os.environ.copy()
    env["SMOKE_PORT"] = str(port)
    env["CONSUL_ADDRESS"] = f"http://127.0.0.1:{consul_port}"
    # Isolate storage from the user's Caddy data and other smoke-test runs.
    env["XDG_DATA_HOME"] = str(workdir / "data")
    env["XDG_CONFIG_HOME"] = str(workdir / "config")
    adapted = subprocess.run(
        [str(binary), "adapt", "--config", str(config), "--adapter", "caddyfile"],
        env=env, capture_output=True, text=True, timeout=5,
    )
    if adapted.returncode:
        raise AssertionError(f"{label} failed to adapt:\n{adapted.stderr}")
    logfile = workdir / f"{label}.log"
    with logfile.open("w") as logs:
        process = subprocess.Popen(
            [str(binary), "run", "--config", str(config), "--adapter", "caddyfile"],
            env=env, stdout=logs, stderr=logs,
        )
        try:
            deadline = time.monotonic() + 5
            while True:
                if process.poll() is not None:
                    raise AssertionError(f"{label}: Caddy exited before listening")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise AssertionError(f"{label}: Caddy did not start in 5 seconds")
                    time.sleep(0.02)
            yield port
        except BaseException:
            logs.flush()
            print(f"--- Caddy log for {label} ---", flush=True)
            print(logfile.read_text(), flush=True)
            raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)


def expect_backend(response, tier):
    status, actual_tier, body = response
    assert status == 200, f"expected 200, got {status} (tier {actual_tier})"
    assert actual_tier == tier, f"expected tier {tier}, got {actual_tier}"
    assert body == b"healthy remote backend\n", f"unexpected backend response: {body!r}"


def run(args):
    binary = Path(args.caddy).resolve()
    fixtures_dir = Path(__file__).resolve().parent
    fixtures = Fixtures()
    scenarios = args.scenario or ["basic", "map", "locality", "strict"]
    with contextlib.ExitStack() as stack:
        workdir = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="caddy-consul-smoke-")))
        backend_port = stack.enter_context(server(backend_handler(fixtures)))
        consul_port = stack.enter_context(server(consul_handler(fixtures)))
        dead = [stack.enter_context(unused_socket()) for _ in range(2)]
        remote = instance(backend_port, "b")
        local_and_remote = [instance(sock.getsockname()[1], "a") for sock in dead] + [remote]
        basic = fixtures_dir / "Caddyfile"
        mapped = fixtures_dir / "Caddyfile.map"
        for scenario in scenarios:
            fixtures.use_entries(local_and_remote if scenario in ("locality", "strict") else [remote])
            config = mapped if scenario == "map" else basic
            if scenario == "strict":
                config = workdir / "Caddyfile.strict"
                config.write_text(basic.read_text().replace("minimum 2", "minimum 2\n\t\t\t\tstrict"))
            before_consul, before_backend = fixtures.counts()
            with caddy(binary, config, consul_port, workdir, scenario) as port:
                if scenario == "basic":
                    expect_backend(get(port), "all")
                elif scenario == "map":
                    for host in ("api.example.test", "web.example.test"):
                        expect_backend(get(port, host), "all")
                    paths = fixtures.request_paths()[before_consul:]
                    assert paths == ["/v1/health/service/api", "/v1/health/service/web"], (
                        f"allowed hosts queried the wrong services: {paths}"
                    )
                    before_unknown = fixtures.counts()[0]
                    status, _, _ = get(port, "unknown.example.test")
                    assert status == 503, f"unknown host should fail closed, got {status}"
                    assert fixtures.counts()[0] == before_unknown, "unknown host queried Consul"
                elif scenario == "locality":
                    for _ in range(5):
                        expect_backend(get(port), "fallback")
                    calls = fixtures.counts()[0] - before_consul
                    assert calls <= args.max_consul_calls, f"5 requests caused {calls} Consul calls"
                elif scenario == "strict":
                    for _ in range(2):
                        status, _, _ = get(port)
                        assert status in (502, 503), f"strict locality unexpectedly returned {status}"
                    assert fixtures.counts()[1] == before_backend, "strict locality reached remote backend"
            calls = fixtures.counts()[0] - before_consul
            print(f"{scenario}: passed ({calls} Consul calls)", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--caddy", required=True, help="Caddy binary built with this module")
    parser.add_argument("--scenario", action="append", choices=("basic", "map", "locality", "strict"))
    parser.add_argument("--max-consul-calls", type=int, default=5,
                        help="maximum discovery calls for 5 locality requests (default: 5)")
    run(parser.parse_args())
