"""Tests for reconnect and unload behaviour."""

from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import CONF_HOST, CONF_PASSWORD, CONF_PORT, CONF_SSL, CONF_USERNAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.nats_io.const import DOMAIN


def _entry(hass: HomeAssistant) -> MockConfigEntry:
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
    return entry


def _client(*, closed: bool = False) -> MagicMock:
    nc = MagicMock()
    nc.subscribe = AsyncMock()
    nc.drain = AsyncMock()
    nc.is_closed = closed
    return nc


async def test_connect_retries_forever(hass: HomeAssistant) -> None:
    """The client must never give up reconnecting."""
    entry = _entry(hass)
    connect = AsyncMock(return_value=_client())
    with patch("custom_components.nats_io.nats.connect", connect):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
    assert connect.call_args.kwargs["max_reconnect_attempts"] == -1
    assert connect.call_args.kwargs["closed_cb"] is not None


async def test_closed_connection_reloads_entry(hass: HomeAssistant) -> None:
    """A permanently closed client triggers an entry reload."""
    entry = _entry(hass)
    connect = AsyncMock(return_value=_client())
    with patch("custom_components.nats_io.nats.connect", connect):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        await connect.call_args.kwargs["closed_cb"]()
        await hass.async_block_till_done()
    assert connect.call_count == 2
    assert entry.state is ConfigEntryState.LOADED


async def test_unload_closed_client_succeeds(hass: HomeAssistant) -> None:
    """Unloading must not raise when the connection is already closed."""
    entry = _entry(hass)
    nc = _client(closed=True)
    with patch("custom_components.nats_io.nats.connect", AsyncMock(return_value=nc)):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        assert await hass.config_entries.async_unload(entry.entry_id)
    nc.drain.assert_not_called()
    assert entry.state is ConfigEntryState.NOT_LOADED
