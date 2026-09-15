# Wire protocol

Every message is a JSON object in a WebSocket **text** frame with a `type` field. Binary
frames are refused with an error. The server never sends anything it was not asked for
except `authenticated`, `resync` and `error`.

## Handshake

1. Client opens the WebSocket.
2. Client sends `auth` as its **first** message, within the auth timeout (10 s default).
3. Server answers `authenticated` or `error` + close.
4. Client subscribes.

Sending anything other than `auth` first, or waiting too long, closes the connection.

## Client → server

| `type` | Fields | Notes |
| --- | --- | --- |
| `auth` | `token: string` | First message only. A second `auth` gets `error/already_authenticated`. |
| `subscribe` | `channels: string[]` | Channels are opaque strings. Duplicates are ignored. |
| `unsubscribe` | `channels: string[]` | Unknown channels are ignored. |
| `ping` | `timestamp: number` | Application-level; answered with `pong` carrying the same value. |

## Server → client

| `type` | Fields | When |
| --- | --- | --- |
| `authenticated` | `subject: string` | Once, after a valid `auth`. Subscribe after this. |
| `subscribed` | `channels: string[]`, `rejected?: {channel, reason}[]` | Per `subscribe`. `channels` lists what is now active. `rejected` is present only when non-empty; `reason` is `unauthorized` or `limit`. |
| `snapshot` | `channel`, `payload` | After `subscribed`, once per newly subscribed channel that has a snapshot. |
| `unsubscribed` | `channels: string[]` | Per `unsubscribe`. |
| `data` | `channel: string`, `payload: any` | One per publish per subscribed channel. |
| `resync` | `channels: string[]` | The upstream lost data. Empty `channels` means everything. Discard state; the next `snapshot` or a re-subscribe restores it. |
| `pong` | `timestamp` | Per `ping`. |
| `error` | `code: string`, `message: string` | `unauthenticated`, `already_authenticated`, `invalid_message`, `protocol`. |

`payload` is whatever the publisher gave the server, embedded verbatim. The server never
parses or re-encodes it.

## Liveness

The server sends a WebSocket **Ping frame** every `heartbeat_interval`. A connection with
no inbound traffic of any kind for `heartbeat_timeout` is closed. Inbound traffic means:
a Pong frame, a text message, a `ping`. Browsers answer Ping frames automatically, so a
browser client that only listens is kept alive without writing any code.

## Backpressure

Each connection has a bounded outbound queue. When it is full the frame is dropped for
that connection and counted. A client that keeps overflowing is closed. Publishing never
blocks on any client.

## Limits

| Limit | Default | On breach |
| --- | --- | --- |
| `max_connections` | 10 000 | TCP connection refused before the WebSocket handshake |
| `max_message_size` | 64 KiB | Connection closed |
| `max_subscriptions` | 256 per connection | Extra channels reported as `rejected/limit` |
| `send_queue` | 256 frames | Frame dropped for that connection |
| `max_consecutive_drops` | 1 000 | Connection closed |

## Example session

```
→ {"type":"auth","token":"…"}
← {"type":"authenticated","subject":"alice"}
→ {"type":"subscribe","channels":["book:BTC-USD","private:x"]}
← {"type":"subscribed","channels":["book:BTC-USD"],"rejected":[{"channel":"private:x","reason":"unauthorized"}]}
← {"type":"snapshot","channel":"book:BTC-USD","payload":{"bids":[…],"asks":[…]}}
← {"type":"data","channel":"book:BTC-USD","payload":{"bids":[[79134.0,0.5]],"asks":[]}}
→ {"type":"ping","timestamp":1758000000000}
← {"type":"pong","timestamp":1758000000000}
```
