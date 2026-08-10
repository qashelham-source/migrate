from __future__ import annotations

import asyncio
from pathlib import Path

import yaml

import app.telegram_client as telegram_client


def test_start_client_waits_through_floodwait_without_a_restart_loop(monkeypatch) -> None:
    waits: list[int] = []
    warnings: list[str] = []

    class StartupFloodWait(Exception):
        def __init__(self, seconds: int) -> None:
            self.value = seconds

    class Client:
        def __init__(self) -> None:
            self.start_calls = 0
            self.stop_calls = 0

        async def start(self) -> None:
            self.start_calls += 1
            if self.start_calls == 1:
                raise StartupFloodWait(7)

        async def stop(self) -> None:
            self.stop_calls += 1

    class Logger:
        def warning(self, message: str, *args: object) -> None:
            warnings.append(message % args)

    async def record_sleep(seconds: int) -> None:
        waits.append(seconds)

    monkeypatch.setattr(telegram_client, "FloodWait", StartupFloodWait)
    monkeypatch.setattr(telegram_client.asyncio, "sleep", record_sleep)

    client = Client()
    asyncio.run(
        telegram_client.start_client_with_floodwait(
            client,  # type: ignore[arg-type]
            label="writer bot",
            logger=Logger(),
        )
    )

    assert client.start_calls == 2
    assert client.stop_calls == 1
    assert waits == [8]
    assert "keeping this service alive" in warnings[0]


def test_compose_keeps_admin_web_and_migration_in_independent_services() -> None:
    compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = compose["services"]
    common = compose["x-migration-common"]

    assert {"migration-manager", "migration-admin", "migration-web"} <= set(services)
    assert "python main.py serve" in "\n".join(services["migration-manager"]["command"])
    assert services["migration-admin"]["command"] == ["python", "main.py", "admin"]
    assert services["migration-web"]["command"] == ["python", "main.py", "web"]
    assert services["migration-admin"]["depends_on"]["migration-manager"]["condition"] == "service_healthy"
    assert "ports" not in services["migration-manager"]
    assert services["migration-web"]["ports"]
    assert common["read_only"] is True
    assert "./data/pyrogram_unknown_errors.txt:/app/unknown_errors.txt" in common["volumes"]

    deploy_script = compose_path.with_name("deploy.sh").read_text(encoding="utf-8")
    assert "touch data/pyrogram_unknown_errors.txt" in deploy_script
    assert "chmod 600 data/pyrogram_unknown_errors.txt" in deploy_script
