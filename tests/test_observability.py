"""Exercise Compose and the Grafana startup boundary, without a Docker daemon."""

from __future__ import annotations

import json
import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def compose_config(**values: str) -> dict:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            str(ROOT / "deploy/docker-compose.yml"),
            "--profile",
            "observability",
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env={"PATH": os.environ["PATH"], **values},
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["services"]


def start_grafana(**values: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["/bin/sh", str(ROOT / "deploy/grafana-entrypoint.sh"), "/usr/bin/env"],
        env={"PATH": "/usr/bin:/bin", **values},
        capture_output=True,
        text=True,
        check=False,
    )


def environment_of(result: subprocess.CompletedProcess) -> dict[str, str]:
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


class ObservabilityPolicyTest(unittest.TestCase):
    def test_default_ports_do_not_publish_metrics_or_dashboard_to_the_network(self):
        services = compose_config()
        for name, port in (("prometheus", 9090), ("grafana", 3000)):
            with self.subTest(service=name):
                self.assertEqual(len(services[name]["ports"]), 1)
                self.assertEqual(services[name]["ports"][0].get("host_ip", "0.0.0.0"), "127.0.0.1")
                self.assertEqual(services[name]["ports"][0]["target"], port)

    def test_anonymous_configuration_has_no_write_role_or_basic_login(self):
        settings = compose_config()["grafana"]["environment"]
        self.assertEqual(settings["GF_AUTH_ANONYMOUS_ORG_ROLE"], "Viewer")
        self.assertEqual(settings["GF_AUTH_BASIC_ENABLED"], "false")
        self.assertEqual(settings["GF_AUTH_DISABLE_LOGIN_FORM"], "true")

    def test_ipv6_loopback_is_rendered_without_ambiguous_short_port_syntax(self):
        grafana = compose_config(FAILSAFE_GRAFANA_BIND_ADDRESS="::1")["grafana"]
        self.assertEqual(grafana["ports"][0]["host_ip"], "::1")
        self.assertEqual(grafana["ports"][0]["target"], 3000)

    def test_explicit_remote_binding_keeps_prometheus_private_and_installs_guard(self):
        services = compose_config(FAILSAFE_GRAFANA_BIND_ADDRESS="0.0.0.0")
        grafana = services["grafana"]
        self.assertEqual(grafana["ports"][0]["host_ip"], "0.0.0.0")
        self.assertEqual(grafana["environment"]["FAILSAFE_GRAFANA_BIND_ADDRESS"], "0.0.0.0")
        self.assertEqual(services["prometheus"]["ports"][0]["host_ip"], "127.0.0.1")
        self.assertEqual(
            grafana["entrypoint"],
            ["/bin/sh", "/etc/failsafe/grafana-entrypoint.sh", "/run.sh"],
        )
        mounts = {mount["target"]: mount for mount in grafana["volumes"]}
        for target, suffix in (
            ("/etc/grafana/provisioning", "/monitoring/grafana/provisioning"),
            ("/var/lib/grafana/dashboards", "/monitoring/grafana/dashboards"),
            ("/etc/failsafe/grafana-entrypoint.sh", "/deploy/grafana-entrypoint.sh"),
        ):
            self.assertTrue(mounts[target]["read_only"])
            self.assertTrue(mounts[target]["source"].endswith(suffix))

    def test_actual_compose_environment_cannot_start_remote_anonymous_grafana(self):
        for password in ("", "fixture-only-remote-password"):
            services = compose_config(
                FAILSAFE_GRAFANA_BIND_ADDRESS="192.0.2.7",
                FAILSAFE_GRAFANA_ADMIN_PASSWORD=password,
            )
            result = start_grafana(**services["grafana"]["environment"])
            if password:
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(environment_of(result)["GF_AUTH_ANONYMOUS_ENABLED"], "false")
            else:
                self.assertEqual(result.returncode, 64)

    def test_local_startup_keeps_viewer_and_replaces_known_initial_password(self):
        result = start_grafana(GF_SECURITY_ADMIN_PASSWORD="admin")
        self.assertEqual(result.returncode, 0, result.stderr)
        settings = environment_of(result)
        self.assertEqual(settings["GF_AUTH_ANONYMOUS_ENABLED"], "true")
        self.assertEqual(settings["GF_AUTH_ANONYMOUS_ORG_ROLE"], "Viewer")
        self.assertEqual(settings["GF_AUTH_BASIC_ENABLED"], "false")
        self.assertEqual(settings["GF_AUTH_DISABLE_LOGIN_FORM"], "true")
        self.assertRegex(settings["GF_SECURITY_ADMIN_PASSWORD"], r"^[0-9a-f]{64}$")

    def test_remote_startup_fails_closed_without_a_configured_password(self):
        for address in ("0.0.0.0", "192.0.2.7", "::", "localhost"):
            for password in (None, "", "admin", "dev-token", "short", " " * 16):
                with self.subTest(address=address, password=password):
                    values = {"FAILSAFE_GRAFANA_BIND_ADDRESS": address}
                    if password is not None:
                        values["GF_SECURITY_ADMIN_PASSWORD"] = password
                    result = start_grafana(**values)
                    self.assertEqual(result.returncode, 64)
                    self.assertIn("configured password", result.stderr)
                    self.assertEqual(result.stdout, "")

    def test_remote_startup_disables_anonymous_access_and_treats_password_as_data(self):
        password = "fixture-$(printf injected)-'long-password"
        result = start_grafana(
            FAILSAFE_GRAFANA_BIND_ADDRESS="0.0.0.0",
            GF_SECURITY_ADMIN_PASSWORD=password,
            GF_AUTH_ANONYMOUS_ENABLED="true",
            GF_AUTH_ANONYMOUS_ORG_ROLE="Admin",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        settings = environment_of(result)
        self.assertEqual(settings["GF_SECURITY_ADMIN_PASSWORD"], password)
        self.assertEqual(settings["GF_AUTH_ANONYMOUS_ENABLED"], "false")
        self.assertEqual(settings["GF_AUTH_ANONYMOUS_ORG_ROLE"], "Viewer")
        self.assertEqual(settings["GF_AUTH_BASIC_ENABLED"], "true")
        self.assertEqual(settings["GF_AUTH_DISABLE_LOGIN_FORM"], "false")
        self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
