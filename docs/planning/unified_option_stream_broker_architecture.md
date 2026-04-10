# Unified Option Stream Broker Architecture

Status: implemented

Last updated: April 10, 2026

## Why This Exists

Current live capture uses two separate API-process brokers:

- [option_quote_capture.py](/Users/adeb/Projects/spreads/src/spreads/services/option_quote_capture.py)
- [option_trade_capture.py](/Users/adeb/Projects/spreads/src/spreads/services/option_trade_capture.py)

That design works functionally, but it is structurally wrong for Alpaca's option stream constraints.

Observed runtime behavior on April 10, 2026:

- intermittent `503` responses from `/internal/market-data/option-trades/capture`
- underlying Alpaca stream auth error `406: connection limit exceeded`
- quote capture `500` responses during API restarts because in-flight capture futures were cancelled during shutdown

The `500` shutdown behavior is a separate operational issue. The main architecture problem is the `406`.

## External Constraint

Verified against Alpaca's official docs on April 10, 2026:

- the option stream URL is `wss://stream.data.alpaca.markets/v1beta1/{feed}`
- the option stream supports both `quotes` and `trades`
- the option stream is `msgpack` only
- most subscriptions, including Algo Trader Plus, allow only `1` active connection to a single endpoint per user

Practical implication:

- opening one websocket for quotes and another websocket for trades against the same option feed is inherently fragile
- a second connection may fail with `406 connection limit exceeded`
- intra-process handoff delays help only a little; they do not solve concurrent requests or overlapping collectors

Canonical sources:

- [WebSocket Stream](https://docs.alpaca.markets/docs/streaming-market-data)
- [Real-time Option Data](https://docs.alpaca.markets/docs/real-time-option-data)

## Goals

- maintain at most one Alpaca option websocket connection per `feed` per API process
- support both `quotes` and `trades` through that single connection
- preserve targeted symbol subscription and unsubscribe behavior
- allow multiple overlapping internal capture requests without opening more Alpaca connections
- keep current collector behavior and storage shape intact during the first cut

## Non-Goals

- distributed cross-process broker coordination
- full-market OPRA ingestion
- changing alert policy or UOA scoring in this phase
- introducing a new persistent streaming service outside the API process in v1

## Target Architecture

### Single Shared Broker

Replace the separate quote and trade brokers with one shared broker:

- `AlpacaOptionStreamBroker`
- one instance per API process
- keyed by `data_base_url + feed`
- owns exactly one websocket connection for that key while captures are active

This broker subscribes to both channels on the same session:

- `quotes`
- `trades`

### Thin Adapters Stay At The Edge

Keep the existing service and API surfaces as thin adapters over the shared broker first:

- `/internal/market-data/option-quotes/capture`
- `/internal/market-data/option-trades/capture`
- `request_option_quote_capture(...)`
- `request_option_trade_capture(...)`

Those entrypoints should not own websocket lifecycle. They should only:

- normalize candidates
- request a capture from the shared broker
- transform captured payloads into persisted quote/trade records

This keeps the first migration narrow and avoids changing collector call sites immediately.

## Core Broker Model

### Active Capture

Each in-flight caller becomes an active capture record with:

- `capture_id`
- `symbols`
- `want_quotes`
- `want_trades`
- `deadline_at`
- `quote_buffer`
- `trade_buffer`
- `future`

The broker should allow overlapping captures with different symbol sets and different channel needs.

### Subscription State

The broker maintains:

- `desired_quote_symbols`
- `desired_trade_symbols`
- `subscribed_quote_symbols`
- `subscribed_trade_symbols`

Each loop iteration computes the set diff and sends:

- `subscribe` for newly needed quote/trade symbols
- `unsubscribe` for symbols no longer needed by any active capture

### Message Router

Incoming websocket payloads are decoded once and routed by:

- channel
- option symbol

For each message:

- quote messages go only to captures requesting quotes for that symbol
- trade messages go only to captures requesting trades for that symbol

This lets multiple concurrent captures share one Alpaca session safely.

## Request Flow

### Quote Capture Request

1. API endpoint receives quote capture request.
2. Quote adapter builds normalized symbol metadata.
3. Adapter calls shared broker with:
   - `symbols`
   - `want_quotes=True`
   - `want_trades=False`
   - `duration_seconds`
4. Broker registers the capture, updates desired subscriptions, and returns buffered quotes at expiry.
5. Adapter converts live quotes into the existing quote-record payload shape.

### Trade Capture Request

1. API endpoint receives trade capture request.
2. Trade adapter builds normalized symbol metadata.
3. Adapter calls shared broker with:
   - `symbols`
   - `want_quotes=False`
   - `want_trades=True`
   - `duration_seconds`
4. Broker returns buffered trades at expiry.
5. Adapter converts live trades into the existing trade-record payload shape.

### Combined Capture

Implemented:

- `/internal/market-data/options/capture`

That endpoint now requests both channels together and returns:

- `quotes`
- `trades`
- per-channel error metadata

The live collector could then issue one combined capture request per cycle instead of two sequential internal API calls.

## Lifecycle

### Connection Startup

When the first active capture appears:

- open the Alpaca websocket
- await `connected`
- authenticate
- begin the broker loop

### Idle Shutdown

When no active captures remain:

- keep the connection alive briefly behind an idle timeout
- if no new captures arrive, close it cleanly

This avoids churn when requests are close together while still releasing the connection during idle periods.

### Reconnect

If the socket drops unexpectedly:

- fail active captures with a typed broker error
- clear subscription state
- allow the next active demand to open a fresh connection

For v1, do not attempt complicated in-place replay or resume.

## Failure Semantics

### Alpaca `406`

Meaning:

- another connection already owns the endpoint budget

Expected remaining causes after this refactor:

- another API replica
- another local dev process
- a third-party app using the same Alpaca market-data websocket

Desired behavior:

- return a controlled degraded error, not a generic exception
- surface it as broker saturation / external connection ownership
- make logs explicit that this is not a scanner-scoring issue

### App Shutdown

Current behavior:

- in-flight capture futures can be cancelled during API restart and bubble up as `500`

Desired behavior:

- translate shutdown cancellation into controlled `503 service_unavailable`
- log as local process shutdown, not feed failure

This is operationally separate from the unified broker, but should be cleaned up during the same refactor.

## Integration Plan

### Phase 1: Internal Broker Unification

- add `AlpacaOptionStreamBroker`
- move websocket connection logic into that broker
- update quote/trade capture services to use it
- keep current internal API endpoints unchanged
- keep current persisted quote/trade record shapes unchanged

Exit condition:

- one API process holds at most one option websocket per feed while still serving quote and trade capture requests

### Phase 2: Collector Simplification

- optionally add one combined internal capture endpoint
- add one combined internal capture endpoint
- replace sequential quote/trade capture requests in the live collector with one combined request
- remove the current handoff sleep once it is no longer needed

Exit condition:

- live collector performs one broker-backed options capture request per cycle

### Phase 3: Visibility And Hardening

- add broker state metrics:
  - active captures
  - desired quote symbol count
  - desired trade symbol count
  - reconnect count
  - `406` count
  - shutdown-cancel count
- expose a small internal health view if needed later

Exit condition:

- operators can tell whether degraded capture is coming from Alpaca ownership, app restart, or empty live flow

## Code Shape

Expected module layout:

- new shared broker module:
  - `/Users/adeb/Projects/spreads/src/spreads/services/option_stream_broker.py`
- existing adapters remain:
  - `/Users/adeb/Projects/spreads/src/spreads/services/option_quote_capture.py`
  - `/Users/adeb/Projects/spreads/src/spreads/services/option_trade_capture.py`
- API wiring remains in:
  - `/Users/adeb/Projects/spreads/apps/api/main.py`

## Why This Is The Right v1

- it directly matches Alpaca's documented stream model
- it removes the self-inflicted quote/trade connection collision inside one API process
- it keeps the thin quote/trade endpoint contracts intact for existing callers
- it still lets the live collector collapse onto one combined internal capture request

## Success Criteria

- trade capture `503` responses caused by `406 connection limit exceeded` drop to near zero in the single-container Docker runtime
- quote and trade capture can overlap safely inside one API process
- live collector no longer depends on timing gaps to avoid self-collision
- restart-related capture cancellations are reported as controlled service-unavailable behavior instead of raw `500`

## Current Status

Implemented on April 10, 2026:

- new shared broker in [option_stream_broker.py](/Users/adeb/Projects/spreads/src/spreads/services/option_stream_broker.py)
- thin quote and trade adapters now reuse that broker in [option_quote_capture.py](/Users/adeb/Projects/spreads/src/spreads/services/option_quote_capture.py) and [option_trade_capture.py](/Users/adeb/Projects/spreads/src/spreads/services/option_trade_capture.py)
- combined internal endpoint and broker-backed adapter in [option_market_data_capture.py](/Users/adeb/Projects/spreads/src/spreads/services/option_market_data_capture.py) and [main.py](/Users/adeb/Projects/spreads/apps/api/main.py)
- live collector cut over to one internal combined capture request in [live_collector.py](/Users/adeb/Projects/spreads/src/spreads/jobs/live_collector.py)
- broker health metrics and internal visibility endpoint in [option_stream_broker.py](/Users/adeb/Projects/spreads/src/spreads/services/option_stream_broker.py) and [main.py](/Users/adeb/Projects/spreads/apps/api/main.py)

Verified runtime behavior after cutover:

- scheduled collector traffic now hits `POST /internal/market-data/options/capture`
- concurrent quote and trade adapter requests return `200 OK` without fresh `406 connection limit exceeded` errors in the post-fix verification window
- restart-time capture cancellations now map to controlled `503` responses instead of raw `500`

Remaining work:

- cross-process or third-party ownership of Alpaca's single stream budget can still produce legitimate `406` degradation outside this API process
