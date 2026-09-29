"""Explicit real-container gate: python3 -m unittest tests.grafana_runtime -v.

Requires an already available grafana/grafana:11.1.0 image. No build or pull,
inherited credentials, developer .env, fixed host ports, or shared database.
"""

from __future__ import annotations

import base64
import http.client
import json
import os
import subprocess
import time
import unittest
import uuid

from tests.test_observability import ROOT, compose_config

# Deliberately public test data. Passing through Compose must not interpret it.
PASSWORD = "fixture-$(printf injected)-'long-password"


def docker(*args: str, values: dict[str, str] | None = None, timeout: int = 30) -> str:
    result = subprocess.run(
        ["docker", *args],
        cwd=ROOT,
        env={"PATH": os.environ["PATH"], **(values or {})},
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"Docker {args[0]} failed ({result.returncode}): {result.stderr}")
    # Docker forwards the container's stderr separately; startup guard messages
    # deliberately go there and must remain visible to refusal assertions.
    output = result.stdout + result.stderr if args[0] == "logs" else result.stdout
    return output.strip()


class GrafanaRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        image = compose_config()["grafana"]["image"]
        if image != "grafana/grafana:11.1.0":
            raise RuntimeError("Update the runtime gate when changing the Grafana image")
        docker("image", "inspect", image, "--format", "{{.Id}}")

    def start(self, **values: str) -> str:
        project = f"failsafe-grafana-test-{uuid.uuid4().hex}"
        name = f"{project}-grafana"
        # Register before starting: even a CLI timeout can leave a created container.
        self.addCleanup(self.remove_owned, project, name)
        docker(
            "compose",
            "--env-file",
            "/dev/null",
            "--project-name",
            project,
            "-f",
            str(ROOT / "deploy/docker-compose.yml"),
            "--profile",
            "observability",
            "run",
            "--detach",
            "--no-deps",
            "--no-TTY",
            "--pull",
            "never",
            "--name",
            name,
            "--publish",
            "127.0.0.1::3000",
            "grafana",
            values=values,
            timeout=45,
        )
        return name

    def remove_owned(self, project: str, name: str):
        # Resolve only this run's unguessable names, and verify their ownership labels.
        containers = docker("ps", "--all", "--quiet", "--filter", f"name=^{name}$")
        for container_id in containers.splitlines():
            label = docker(
                "inspect",
                container_id,
                "--format",
                '{{index .Config.Labels "com.docker.compose.project"}}',
            )
            self.assertEqual(label, project, "Refusing to remove another project's container")
            docker("rm", "--force", "--volumes", container_id)
        networks = docker(
            "network", "ls", "--quiet", "--filter", f"label=com.docker.compose.project={project}"
        )
        for network_id in networks.splitlines():
            docker("network", "rm", network_id)

    def request(self, port, path, *, method="GET", password=None, body=None):
        headers = {"Accept": "application/json"}
        if password is not None:
            token = base64.b64encode(f"admin:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        payload = None
        if body is not None:
            payload = json.dumps(body)
            headers["Content-Type"] = "application/json"
        # HTTPConnection does not consult environment proxy settings or cookie stores.
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        try:
            connection.request(method, path, body=payload, headers=headers)
            response = connection.getresponse()
            data = response.read()
            return response.status, json.loads(data) if data else None
        finally:
            connection.close()

    def healthy_port(self, name: str) -> int:
        ports = json.loads(docker("inspect", name, "--format", "{{json .NetworkSettings.Ports}}"))
        self.assertEqual(set(ports), {"3000/tcp"})
        self.assertEqual(len(ports["3000/tcp"]), 1)
        self.assertEqual(ports["3000/tcp"][0]["HostIp"], "127.0.0.1")
        port = int(ports["3000/tcp"][0]["HostPort"])
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                status, body = self.request(port, "/api/health")
                if status == 200 and body.get("database") == "ok":
                    self.assertEqual(body["version"], "11.1.0")
                    return port
            except (OSError, http.client.HTTPException, ValueError):
                pass
            state = docker("inspect", name, "--format", "{{.State.Status}}")
            self.assertEqual(state, "running", "Grafana exited before becoming healthy")
            time.sleep(1)
        self.fail("Grafana did not become healthy within 90 seconds")

    def assert_provisioned(self, port, *, password=None):
        status, dashboard = self.request(port, "/api/dashboards/uid/failsafe", password=password)
        self.assertEqual(status, 200)
        self.assertEqual(dashboard["dashboard"]["uid"], "failsafe")
        self.assertEqual(dashboard["dashboard"]["title"], "FailSafe Gateway")
        self.assertTrue(dashboard["meta"]["provisioned"])
        status, datasource = self.request(
            port, "/api/datasources/uid/prometheus", password=password
        )
        self.assertEqual(status, 200)
        self.assertEqual(datasource["uid"], "prometheus")
        self.assertEqual(datasource["type"], "prometheus")
        self.assertEqual(datasource["url"], "http://prometheus:9090")
        self.assertTrue(datasource["readOnly"])

    def test_default_local_viewer_can_read_but_cannot_write_or_log_in_as_admin(self):
        name = self.start(FAILSAFE_GRAFANA_ADMIN_PASSWORD=PASSWORD)
        port = self.healthy_port(name)
        self.assert_provisioned(port)
        status, body = self.request(
            port,
            "/api/dashboards/db",
            method="POST",
            body={"dashboard": {"title": "Must not be created", "schemaVersion": 39}},
        )
        self.assertIn(status, (401, 403), body)
        for password in (None, "admin", PASSWORD):
            with self.subTest(password=password):
                status, body = self.request(port, "/api/admin/settings", password=password)
                self.assertIn(status, (401, 403), body)
        status, body = self.request(
            port, "/login", method="POST", body={"user": "admin", "password": PASSWORD}
        )
        # With basic authentication disabled, Grafana 11.1 refuses the login
        # client itself, rather than treating the supplied password as invalid.
        self.assertEqual(status, 400, body)
        self.assertEqual(body.get("messageId"), "auth.client.notConfigured")

    def assert_auth_required(self, port):
        for path in ("/api/dashboards/uid/failsafe", "/api/datasources/uid/prometheus"):
            status, body = self.request(port, path)
            self.assertEqual(status, 401, body)
        self.assert_provisioned(port, password=PASSWORD)
        status, user = self.request(port, "/api/user", password=PASSWORD)
        self.assertEqual(status, 200, user)
        self.assertEqual(user["login"], "admin")
        self.assertTrue(user["isGrafanaAdmin"])
        status, body = self.request(port, "/api/user", password="wrong-password")
        self.assertEqual(status, 401, body)

    def test_explicit_local_auth_accepts_the_literal_password_and_rejects_anonymous(self):
        name = self.start(
            FAILSAFE_GRAFANA_REQUIRE_AUTH="1", FAILSAFE_GRAFANA_ADMIN_PASSWORD=PASSWORD
        )
        self.assert_auth_required(self.healthy_port(name))

    def test_remote_policy_requires_auth_even_when_opt_in_is_zero(self):
        # Exercise remote policy while publishing the test container only on host loopback.
        name = self.start(
            FAILSAFE_GRAFANA_BIND_ADDRESS="0.0.0.0",
            FAILSAFE_GRAFANA_REQUIRE_AUTH="0",
            FAILSAFE_GRAFANA_ADMIN_PASSWORD=PASSWORD,
        )
        self.assert_auth_required(self.healthy_port(name))

    def test_unsafe_configuration_exits_before_grafana_starts(self):
        for values, diagnostic in (
            ({"FAILSAFE_GRAFANA_BIND_ADDRESS": "0.0.0.0"}, "configured password"),
            ({"FAILSAFE_GRAFANA_REQUIRE_AUTH": "1"}, "configured password"),
            ({"FAILSAFE_GRAFANA_REQUIRE_AUTH": "true"}, "FAILSAFE_GRAFANA_REQUIRE_AUTH"),
            ({"FAILSAFE_GRAFANA_REQUIRE_AUTH": ""}, "FAILSAFE_GRAFANA_REQUIRE_AUTH"),
        ):
            with self.subTest(values=values):
                name = self.start(FAILSAFE_GRAFANA_ADMIN_PASSWORD="short", **values)
                self.assertEqual(docker("wait", name, timeout=15), "64")
                self.assertIn(diagnostic, docker("logs", name))


if __name__ == "__main__":
    unittest.main()
