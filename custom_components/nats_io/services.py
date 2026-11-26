"""Actions for the NATS integration: publish (core or JetStream) and fetch."""

from __future__ import annotations

import json
import logging
from typing import Any

import voluptuous as vol
from nats.errors import Error as NatsError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.errors import Error as JetStreamError

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SERVICE_PUBLISH = "publish"
SERVICE_FETCH = "fetch"

ATTR_CONFIG_ENTRY_ID = "config_entry_id"
ATTR_SUBJECT = "subject"
ATTR_PAYLOAD = "payload"
ATTR_HEADERS = "headers"
ATTR_JETSTREAM = "jetstream"
ATTR_STREAM = "stream"
ATTR_CONSUMER = "consumer"
ATTR_BATCH = "batch"
ATTR_TIMEOUT = "timeout"
ATTR_ACK = "ack"

MAX_BATCH = 100
DEFAULT_BATCH = 10
DEFAULT_FETCH_TIMEOUT = 2.0

PUBLISH_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string,
        vol.Required(ATTR_SUBJECT): cv.string,
        vol.Required(ATTR_PAYLOAD): vol.Any(dict, list, cv.string, int, float, bool),
        vol.Optional(ATTR_HEADERS): vol.Schema({cv.string: cv.string}),
        vol.Optional(ATTR_JETSTREAM, default=False): cv.boolean,
    }
)

FETCH_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_CONFIG_ENTRY_ID): cv.string,
        vol.Required(ATTR_STREAM): cv.string,
        vol.Required(ATTR_CONSUMER): cv.string,
        vol.Optional(ATTR_BATCH, default=DEFAULT_BATCH): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=MAX_BATCH)
        ),
        vol.Optional(ATTR_TIMEOUT, default=DEFAULT_FETCH_TIMEOUT): vol.All(
            vol.Coerce(float), vol.Range(min=0.1, max=30)
        ),
        vol.Optional(ATTR_ACK, default=True): cv.boolean,
    }
)


def _encode_payload(payload: Any) -> bytes:
    """Encode an action payload: strings go out verbatim, anything else as JSON."""
    if isinstance(payload, str):
        return payload.encode()
    return json.dumps(payload).encode()


def _decode_payload(data: bytes) -> Any:
    """Decode message bytes as JSON when possible, else as text."""
    text = data.decode(errors="replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def _get_client_entry(hass: HomeAssistant, call: ServiceCall) -> ConfigEntry:
    """Return the loaded config entry the action should use."""
    loaded = hass.config_entries.async_loaded_entries(DOMAIN)
    if not loaded:
        raise ServiceValidationError(
            "The NATS integration is not loaded", translation_domain=DOMAIN,
            translation_key="not_loaded",
        )
    entry_id = call.data.get(ATTR_CONFIG_ENTRY_ID)
    if entry_id is None:
        if len(loaded) > 1:
            raise ServiceValidationError(
                f"Several NATS entries are loaded; pass {ATTR_CONFIG_ENTRY_ID}",
                translation_domain=DOMAIN,
                translation_key="entry_required",
            )
        return loaded[0]
    for entry in loaded:
        if entry.entry_id == entry_id:
            return entry
    raise ServiceValidationError(
        f"No loaded NATS entry with id {entry_id}",
        translation_domain=DOMAIN,
        translation_key="entry_not_found",
    )


async def _async_publish(hass: HomeAssistant, call: ServiceCall) -> dict[str, Any]:
    """Publish a message, optionally through JetStream, returning the ack."""
    nc = _get_client_entry(hass, call).runtime_data
    subject: str = call.data[ATTR_SUBJECT]
    data = _encode_payload(call.data[ATTR_PAYLOAD])
    headers: dict[str, str] | None = call.data.get(ATTR_HEADERS)

    try:
        if not call.data[ATTR_JETSTREAM]:
            await nc.publish(subject, data, headers=headers)
            return {}
        ack = await nc.jetstream().publish(subject, data, headers=headers)
    except (NatsError, JetStreamError) as err:
        raise HomeAssistantError(f"Publishing to {subject} failed: {err!r}") from err
    return {"stream": ack.stream, "seq": ack.seq, "duplicate": bool(ack.duplicate)}


def _message_to_dict(msg: Any) -> dict[str, Any]:
    """Convert a JetStream message to a JSON-safe action response entry."""
    meta = msg.metadata
    timestamp = getattr(meta, "timestamp", None)
    return {
        "subject": msg.subject,
        "data": _decode_payload(msg.data),
        "headers": dict(msg.headers) if msg.headers else {},
        "seq": meta.sequence.stream,
        "num_delivered": meta.num_delivered,
        "timestamp": timestamp.isoformat() if timestamp else None,
    }


async def _async_fetch(hass: HomeAssistant, call: ServiceCall) -> dict[str, Any]:
    """Pull up to ``batch`` messages from a durable consumer and ack them."""
    nc = _get_client_entry(hass, call).runtime_data
    stream: str = call.data[ATTR_STREAM]
    consumer: str = call.data[ATTR_CONSUMER]
    try:
        psub = await nc.jetstream().pull_subscribe_bind(consumer, stream=stream)
        # Cap the pull at what the consumer can deliver so a short queue returns
        # at once instead of waiting out the timeout for a full batch. Count
        # num_ack_pending too: a message delivered but never acked is redelivered
        # after ack_wait, and JetStream counts it there, not in num_pending.
        info = await psub.consumer_info()
        count = min(call.data[ATTR_BATCH], info.num_pending + info.num_ack_pending)
        if count < 1:
            return {"messages": []}
        try:
            msgs = await psub.fetch(count, timeout=call.data[ATTR_TIMEOUT])
        except NatsTimeoutError:
            return {"messages": []}
        result = [_message_to_dict(msg) for msg in msgs]
        if call.data[ATTR_ACK]:
            for msg in msgs:
                await msg.ack()
    except (NatsError, JetStreamError) as err:
        raise HomeAssistantError(
            f"Fetching from {stream}/{consumer} failed: {err!r}"
        ) from err
    return {"messages": result}


def async_setup_services(hass: HomeAssistant) -> None:
    """Register the integration's actions (once, from async_setup)."""

    async def handle_publish(call: ServiceCall) -> dict[str, Any]:
        return await _async_publish(hass, call)

    async def handle_fetch(call: ServiceCall) -> dict[str, Any]:
        return await _async_fetch(hass, call)

    hass.services.async_register(
        DOMAIN,
        SERVICE_PUBLISH,
        handle_publish,
        schema=PUBLISH_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_FETCH,
        handle_fetch,
        schema=FETCH_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
