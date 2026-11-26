"""Live tests against a real NATS broker (see docker-compose.yml).

Skipped unless NATS_LIVE=1. Run with: docker compose up -d && NATS_LIVE=1 pytest tests/
"""

import asyncio
import json
import os

import nats
import pytest

from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_SSL, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_mock_service

from custom_components.nats_io.const import DOMAIN

pytestmark = [
    pytest.mark.skipif(not os.environ.get("NATS_LIVE"), reason="NATS_LIVE not set"),
    pytest.mark.usefixtures("socket_enabled"),
]

# 127.0.0.1, not localhost: pytest-socket only allows that host
HOST = "127.0.0.1"
PORT = int(os.environ.get("NATS_PORT", "4222"))
USER = PASSWORD = "ha"


@pytest.fixture
async def nc():
    """Return a NATS client acting as the external system."""
    client = await nats.connect(f"nats://{USER}:{PASSWORD}@{HOST}:{PORT}")
    yield client
    await client.drain()


@pytest.fixture
async def entry(hass: HomeAssistant):
    """Set up the integration against the live broker."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_HOST: HOST,
            CONF_PORT: PORT,
            CONF_USERNAME: USER,
            CONF_PASSWORD: PASSWORD,
            CONF_SSL: False,
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    yield entry
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_state_published(hass: HomeAssistant, entry, nc) -> None:
    """State changes arrive on ha.state.<area>.<entity_id>."""
    sub = await nc.subscribe("ha.state.>")
    await nc.flush()

    hass.states.async_set("light.living_room", "on")
    msg = await sub.next_msg(timeout=2)

    assert msg.subject == "ha.state.no_area.light.living_room"
    assert json.loads(msg.data)["state"] == "on"


async def test_state_request(hass: HomeAssistant, entry, nc) -> None:
    """ha.request.state replies with the current state."""
    hass.states.async_set("light.living_room", "off")

    msg = await nc.request(
        "ha.request.state", json.dumps({"entity_id": "light.living_room"}).encode(), timeout=2
    )

    assert json.loads(msg.data)["state"] == "off"


async def test_service_command(hass: HomeAssistant, entry, nc) -> None:
    """ha.service messages call the HA service."""
    calls = async_mock_service(hass, "light", "turn_on")

    await nc.publish(
        "ha.service",
        json.dumps({"action": "light.turn_on", "target": {"entity_id": "light.living_room"}}).encode(),
    )
    await nc.flush()
    async with asyncio.timeout(2):
        while not calls:
            await asyncio.sleep(0.05)

    assert calls[0].data["entity_id"] == "light.living_room"


@pytest.fixture
async def queue(nc):
    """Create a stream and a durable pull consumer, removed afterwards."""
    js = nc.jetstream()
    await js.add_stream(name="LIVE_Q", subjects=["live.queue"])
    await js.add_consumer("LIVE_Q", durable_name="digest", ack_policy="explicit")
    yield
    await js.delete_stream("LIVE_Q")


async def test_publish_jetstream_then_fetch(hass: HomeAssistant, entry, queue) -> None:
    """publish(jetstream) lands in the stream; fetch returns it once, then the queue is empty."""
    ack = await hass.services.async_call(
        DOMAIN,
        "publish",
        {"subject": "live.queue", "payload": {"message": "hello"}, "jetstream": True},
        blocking=True,
        return_response=True,
    )
    assert ack["stream"] == "LIVE_Q"
    assert ack["seq"] == 1

    first = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "LIVE_Q", "consumer": "digest"},
        blocking=True,
        return_response=True,
    )
    assert [m["data"] for m in first["messages"]] == [{"message": "hello"}]
    assert first["messages"][0]["seq"] == 1

    second = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "LIVE_Q", "consumer": "digest"},
        blocking=True,
        return_response=True,
    )
    assert second == {"messages": []}


async def test_fetch_ack_false_redelivers(hass: HomeAssistant, entry, queue, nc) -> None:
    """ack: false leaves the message pending on the consumer."""
    await nc.jetstream().publish("live.queue", b"peek me")

    peeked = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "LIVE_Q", "consumer": "digest", "ack": False},
        blocking=True,
        return_response=True,
    )
    assert [m["data"] for m in peeked["messages"]] == ["peek me"]

    info = await nc.jetstream().consumer_info("LIVE_Q", "digest")
    assert info.num_ack_pending == 1


async def test_fetch_missing_consumer_errors(hass: HomeAssistant, entry, queue) -> None:
    """An unknown consumer raises an HA error."""
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "LIVE_Q", "consumer": "nope"},
            blocking=True,
            return_response=True,
        )


async def test_publish_jetstream_without_stream_errors(hass: HomeAssistant, entry) -> None:
    """A JetStream publish to a subject no stream captures raises an HA error."""
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "publish",
            {"subject": "no.stream.here", "payload": "x", "jetstream": True},
            blocking=True,
            return_response=True,
        )


async def test_fetch_redelivers_unacked_after_ack_wait(hass: HomeAssistant, entry, nc) -> None:
    """A peeked (ack: false) message comes back from fetch once ack_wait passes."""
    from nats.js.api import ConsumerConfig

    js = nc.jetstream()
    await js.add_stream(name="LIVE_R", subjects=["live.redeliver"])
    await js.add_consumer(
        "LIVE_R",
        ConsumerConfig(durable_name="digest", ack_policy="explicit", ack_wait=1),
    )
    try:
        await js.publish("live.redeliver", b"again")
        peek = await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "LIVE_R", "consumer": "digest", "ack": False},
            blocking=True,
            return_response=True,
        )
        assert peek["messages"][0]["num_delivered"] == 1

        await asyncio.sleep(1.5)
        again = await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "LIVE_R", "consumer": "digest"},
            blocking=True,
            return_response=True,
        )
        assert [m["data"] for m in again["messages"]] == ["again"]
        assert again["messages"][0]["num_delivered"] == 2
    finally:
        await js.delete_stream("LIVE_R")
