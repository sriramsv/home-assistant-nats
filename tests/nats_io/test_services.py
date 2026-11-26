"""Test the nats_io.publish and nats_io.fetch actions."""

import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import voluptuous as vol
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import APIError, NoStreamResponseError, NotFoundError

from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_SSL, CONF_USERNAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.nats_io.const import DOMAIN


def _msg(subject: str, data: bytes, seq: int, headers: dict | None = None) -> MagicMock:
    """Return a mock JetStream message."""
    msg = MagicMock()
    msg.subject = subject
    msg.data = data
    msg.headers = headers
    msg.ack = AsyncMock()
    msg.metadata = SimpleNamespace(
        sequence=SimpleNamespace(stream=seq, consumer=seq),
        num_delivered=1,
        timestamp=None,
    )
    return msg


@pytest.fixture
def nc() -> MagicMock:
    """Return a mock NATS client with a mock JetStream context."""
    client = MagicMock()
    client.subscribe = AsyncMock()
    client.publish = AsyncMock()
    client.drain = AsyncMock()
    client.is_closed = False
    client.jetstream.return_value = MagicMock()
    return client


@pytest.fixture
async def entry(hass: HomeAssistant, nc: MagicMock) -> AsyncGenerator[MockConfigEntry]:
    """Set up the integration with a mocked NATS client."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        data={
            CONF_HOST: "localhost",
            CONF_PORT: 4222,
            CONF_USERNAME: "u",
            CONF_PASSWORD: "p",
            CONF_SSL: False,
        },
    )
    entry.add_to_hass(hass)
    with patch("custom_components.nats_io.nats.connect", AsyncMock(return_value=nc)):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        yield entry
        await hass.config_entries.async_unload(entry.entry_id)


async def test_services_registered(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    """Both actions exist once the integration is set up."""
    assert hass.services.has_service(DOMAIN, "publish")
    assert hass.services.has_service(DOMAIN, "fetch")


async def test_publish_core(hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock) -> None:
    """A core publish sends the JSON payload and returns an empty response."""
    result = await hass.services.async_call(
        DOMAIN,
        "publish",
        {"subject": "voice.queue", "payload": {"message": "hi"}},
        blocking=True,
        return_response=True,
    )

    nc.publish.assert_awaited_once_with("voice.queue", b'{"message": "hi"}', headers=None)
    assert result == {}


async def test_publish_string_payload_is_sent_verbatim(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A string payload is not JSON-encoded a second time."""
    await hass.services.async_call(
        DOMAIN,
        "publish",
        {"subject": "voice.queue", "payload": "hello"},
        blocking=True,
        return_response=True,
    )

    nc.publish.assert_awaited_once_with("voice.queue", b"hello", headers=None)


async def test_publish_jetstream_returns_ack(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A JetStream publish returns the stream, sequence and duplicate flag."""
    js = nc.jetstream.return_value
    js.publish = AsyncMock(
        return_value=SimpleNamespace(stream="VOICE", seq=7, duplicate=False)
    )

    result = await hass.services.async_call(
        DOMAIN,
        "publish",
        {
            "subject": "voice.queue",
            "payload": {"a": 1},
            "headers": {"Nats-Msg-Id": "abc"},
            "jetstream": True,
        },
        blocking=True,
        return_response=True,
    )

    js.publish.assert_awaited_once_with(
        "voice.queue", b'{"a": 1}', headers={"Nats-Msg-Id": "abc"}
    )
    assert result == {"stream": "VOICE", "seq": 7, "duplicate": False}


async def test_publish_jetstream_no_stream_raises(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A subject with no stream surfaces as an HA error, not a silent drop."""
    nc.jetstream.return_value.publish = AsyncMock(side_effect=NoStreamResponseError())

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "publish",
            {"subject": "nope", "payload": "x", "jetstream": True},
            blocking=True,
            return_response=True,
        )


async def test_fetch_returns_and_acks_messages(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """Fetch returns decoded messages and acks them by default."""
    m1 = _msg("voice.queue", b'{"message": "one"}', 1, {"k": "v"})
    m2 = _msg("voice.queue", b"plain text", 2)
    psub = MagicMock()
    psub.consumer_info = AsyncMock(return_value=SimpleNamespace(num_pending=2, num_ack_pending=0))
    psub.fetch = AsyncMock(return_value=[m1, m2])
    js = nc.jetstream.return_value
    js.pull_subscribe_bind = AsyncMock(return_value=psub)

    result = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "VOICE", "consumer": "digest", "batch": 10},
        blocking=True,
        return_response=True,
    )

    js.pull_subscribe_bind.assert_awaited_once_with("digest", stream="VOICE")
    psub.fetch.assert_awaited_once()
    assert psub.fetch.await_args.args[0] == 2  # min(batch, pending)
    assert [m["data"] for m in result["messages"]] == [{"message": "one"}, "plain text"]
    assert result["messages"][0]["headers"] == {"k": "v"}
    assert result["messages"][0]["seq"] == 1
    m1.ack.assert_awaited_once()
    m2.ack.assert_awaited_once()


async def test_fetch_ack_false_leaves_messages(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """ack: false peeks; messages are redelivered after ack_wait."""
    m1 = _msg("voice.queue", b"x", 1)
    psub = MagicMock()
    psub.consumer_info = AsyncMock(return_value=SimpleNamespace(num_pending=1, num_ack_pending=0))
    psub.fetch = AsyncMock(return_value=[m1])
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(return_value=psub)

    result = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "VOICE", "consumer": "digest", "ack": False},
        blocking=True,
        return_response=True,
    )

    assert len(result["messages"]) == 1
    m1.ack.assert_not_awaited()


async def test_fetch_empty_queue_does_not_wait(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """With nothing pending the action returns at once without a pull."""
    psub = MagicMock()
    psub.consumer_info = AsyncMock(return_value=SimpleNamespace(num_pending=0, num_ack_pending=0))
    psub.fetch = AsyncMock()
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(return_value=psub)

    result = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "VOICE", "consumer": "digest"},
        blocking=True,
        return_response=True,
    )

    assert result == {"messages": []}
    psub.fetch.assert_not_awaited()


async def test_fetch_includes_redeliverable_messages(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """Unacked messages past ack_wait are redelivered, so they must be pulled too.

    JetStream counts them in num_ack_pending, not num_pending.
    """
    m1 = _msg("voice.queue", b"again", 1)
    psub = MagicMock()
    psub.consumer_info = AsyncMock(
        return_value=SimpleNamespace(num_pending=0, num_ack_pending=1)
    )
    psub.fetch = AsyncMock(return_value=[m1])
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(return_value=psub)

    result = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "VOICE", "consumer": "digest"},
        blocking=True,
        return_response=True,
    )

    psub.fetch.assert_awaited_once()
    assert psub.fetch.await_args.args[0] == 1
    assert [m["data"] for m in result["messages"]] == ["again"]


async def test_fetch_timeout_returns_empty(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A pull that times out is an empty result, not an error."""
    psub = MagicMock()
    psub.consumer_info = AsyncMock(return_value=SimpleNamespace(num_pending=3, num_ack_pending=0))
    psub.fetch = AsyncMock(side_effect=NatsTimeoutError())
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(return_value=psub)

    result = await hass.services.async_call(
        DOMAIN,
        "fetch",
        {"stream": "VOICE", "consumer": "digest"},
        blocking=True,
        return_response=True,
    )

    assert result == {"messages": []}


async def test_fetch_unknown_consumer_raises(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A missing stream or consumer is reported to the caller."""
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(side_effect=NotFoundError())

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "VOICE", "consumer": "missing"},
            blocking=True,
            return_response=True,
        )


async def test_fetch_permission_denied_raises(
    hass: HomeAssistant, entry: MockConfigEntry, nc: MagicMock
) -> None:
    """A broker-side API error becomes an HA error."""
    nc.jetstream.return_value.pull_subscribe_bind = AsyncMock(
        side_effect=APIError(code=403, description="permissions violation")
    )

    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "VOICE", "consumer": "digest"},
            blocking=True,
            return_response=True,
        )


async def test_fetch_batch_is_capped(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    """Batch sizes outside 1..100 are rejected by the schema."""
    with pytest.raises(vol.Invalid):
        await hass.services.async_call(
            DOMAIN,
            "fetch",
            {"stream": "VOICE", "consumer": "digest", "batch": 1000},
            blocking=True,
            return_response=True,
        )


async def test_actions_need_a_loaded_entry(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    """After the entry unloads the actions explain why they cannot run."""
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "publish",
            {"subject": "a", "payload": "b"},
            blocking=True,
            return_response=True,
        )
