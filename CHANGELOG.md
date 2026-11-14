# Changelog

All notable changes to this image will be documented here.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this image follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

When you release a new version, append a section here and bump `version` in `package.json` before tagging. Consumers read this file to decide whether the new version is safe to roll.

## [Unreleased]

Change streams are now opened per collection instead of per query subscription, cutting the server-side oplog scanning that dominated slow-op time under heavy writes. Sends are scheduled per POST against a memory budget sized from the heap, so slow or hung clients no longer hold up fast ones or exhaust memory.

### Changed
- **Matching** runs in process where it agrees exactly with MongoDB, else on a dedicated server-side stream. `ObjectId` / `Date` operands and `$elemMatch` paths now match, so a subscription may get more events than under 1.2.x, or fewer with `$ne` / `$nin`. A `Long` or `Decimal128` value with no exact double fails closed under range operators and negations: it does not match them in process, even where MongoDB would. Changes are matched in 5ms slices shared by all collections, and a change's message to each subscription is queued as soon as that subscription matches, so a change with many subscriptions does not hold up sending. A collection with over 1000 changes, or about 20s of matching, waiting to be matched stops reading until it catches up, with a `warn`.
- **Sending** is scheduled per POST (a whole message or one chunk). Healthy connections take turns on 64 send slots; new and slow ones share at most 16 while a healthy one waits, and slow ones take the last 8 free slots only while they hold fewer than 4. An attempt that runs well past the connection's usual time is cut: a chunk is resent on a later turn, and a whole message keeps running without its slot, delivered once if it is answered, while the connection's next message waits for it (up to 20 per slot and 32 MB; past that it is resent). Its time counts from a connected socket, so DNS, TCP and TLS setup never cause a cut; a send with no connection after 3s is retried, and the 5s send timeout runs from connect. Gateway hostnames are resolved through a 30s in-process cache. One message body is built per change and projection and shared by its subscribers; messages are byte-identical to 1.2.x.
- **Slow clients are disconnected** with `DeleteConnection` when what only they hold has stayed over 60s of sending for 10s, when their oldest message has spent 60s sending (connecting, in flight or waiting to be retried), when a send times out 5s after connecting, or, while deliveries hold more than the memory budget, when they have been slow for 10s, or at once while a message has waited 1s for memory. Slow means a POST latency over 1s and over twice that of the fastest tenth of recently active connections (the 10th fastest past 100), on its recent POSTs and its latest one alike. A connection that keeps up is never disconnected for the budget. A disconnected client is sent nothing more; it leaves its subscriptions once the gateway confirms the close, or after 2h, across restarts. A collection stops reading when the change it reads would take the heap deliveries hold past 1.25 times the budget, and reads it again once they drain below its resume line; one that has waited 10s resumes first. When disconnecting is not enough, the stream holding the most heap also pauses, provided it holds at least an eighth of the budget and its largest change. Each pause `warn` estimates how long it will last ("P about Ns"). Message bodies are built while they fit the budget, in the order they asked, so a change waiting for room holds up only its own collection's matching.
- **Hung connections are disconnected sooner**: a connection whose cut attempts have gone unanswered for over 1s gets one uncut attempt, and when that attempt goes unanswered for more than 2s, or twice what the fastest tenth of the same gateway endpoint's sends of the last 3s took if that is longer, the connection is disconnected with a `warn` that it "went unanswered for Nms while others answered within Mms". With no send to that endpoint completed in the last 3s, the 5s send timeout applies as before. A client that answers within 1s is never disconnected this way, and a slowdown of a whole endpoint to 2-5s disconnects no one; a slowdown of only some connections on an endpoint whose other sends stay fast disconnects those that answer after 2s.
- **Failed sends** are retried by the fanout, not the SDK, and not dropped: after a 429, a 5xx, a 403, an unreachable gateway, a reset socket or no connection within 3s, the connection waits 100ms, 400ms, 1s, 2s, 4s, then 5s between tries, within a retry budget shared by all connections, until its oldest message has spent 60s sending. Failures `warn` once, then with a count every 10s. A 403 to a connection's own send is a strike unless a different connection on the same gateway endpoint got a 403 (to a send, a ping or a `DeleteConnection`) in the last second; three strikes in a row disconnect it with "403 not shared by the endpoint", and a successful send of its own, or a 403 that is not a strike, starts the count again. Another connection's success does not, and a ping's 403 is never a strike. Any other 4xx disconnects the connection at once, a 410 removes its subscriptions at once, and `ExpiredTokenException` exits the process. Pings, and the first message to a subscriber whose endpoint is not this gateway's, are tried once; that subscriber is then removed from every subscription with a `warn`.
- **Memory budget** is a third of the heap left once the subscriptions are loaded, between 16 MB and 256 MB, and follows the subscription count. A change that cannot be delivered within it is skipped with a `warn`, as is one over 16 MB.
- **A failing collection no longer exits the process**; it is retried with backoff up to 60s, logged at `error` on each try when the error will not pass by itself (such as a query the server rejects), and a dedicated stream reopens after its collection is dropped or renamed. Streams open at most 4 at a time, with batch sizes from their recent change sizes. A stream whose resume position has left the oplog disconnects its subscribers, so they refetch, and reopens from now. A collection with nothing left to send saves the server's resume position every 45 to 75s, so a quiet collection keeps its place in the oplog across a restart.
- **A dedicated stream for a filter the matcher cannot run** no longer carries the document from the server; it is looked up before the change is matched, once per change however many dedicated streams read it, and admitted against the memory budget like a change. A lookup that keeps failing is retried for 31s, then the change goes out as its ids-only update with a `warn`; while lookups keep failing, later changes get one try each. A document too large for the budget also sends the ids-only update.
- **The `querySubscriptions` watcher reloads every subscription** at startup, on invalidate and when its resume position is lost, so changes made during startup or while it was down are not missed.
- **A subscriber joining an existing subscription** gets the changes from a second before it joined, as a new subscription already did, ahead of any later change. When part of them was already trimmed or evicted for memory, it is disconnected so it refetches, 10 at once and then one a second, across restarts.
- **Shutdown on `SIGTERM` finishes within 8s**; changes still being sent are redelivered after the restart. Keep the stop timeout at 10s or more.

### Security
- Bump mongoose to 7.8.13 and ip-address to 10.7.6; remove unused zod.

### Notes for consumers
- **Wire protocol**: unchanged.
- **Database**: the task's Mongo user needs read and write access, including creating indexes, to three collections the fanout owns. `subscriberFanoutCursors` holds the resume tokens. `subscriberFanoutCloses` holds the connections it closed until the gateway confirms: the fanout upserts one `{endpoint, connectionId, firstAttemptAt}` record per close and deletes it on confirmation; the collection has a unique index on `{endpoint, connectionId}` and a 2h TTL index on `firstAttemptAt`. `subscriberFanoutTombstones` is new: one `{_id, endpoint, connectionId, reason, expiresAt}` document per connection the fanout closed, found gone (410) or removed as another gateway's in the last 2h, removed by a TTL index on `expiresAt`, so those connections are neither sent to nor re-added, across restarts. A 410 is how most departed clients are found, so expect about one tombstone write per disconnect. The startup `info` line reports the unconfirmed closes and the tombstones loaded.
- **Gateway**: the task role needs `execute-api:ManageConnections` for `POST` and `DELETE` on `@connections/*`. Up to 21 times the send slots plus 8 sends (1352 by default) can be open to the gateway at once, counting whole messages left running past a cut, through two socket pools, one for new and slow clients; closes use 16 more.
- **Memory**: give the heap a limit of at least what it holds after startup plus three times the budget, and the container about twice the heap limit. At about 10.6 KB per subscription, 20k subscriptions at a 16 MB budget need about a 600 MB container. The startup `info` line reports the budget, the heap per subscription and the largest document delivered; an `error` names the heap limit needed when the heap is short of that, including when the budget is set with `maxQueuedBytes`. During the startup load, an `error` after the first 1000 subscriptions names the heap limit the whole load needs when it would leave the budget under 16 MB.
- **Send slots**: set `WEBSOCKET_SEND_CONCURRENCY` to change the 64 POSTs in flight at once. More slots serve more clients per turn, at the cost of more concurrent requests against the account's management API rate limit.
- **Pool**: 100 connections by default (was 2); set `MONGODB_MAX_POOL_SIZE` above the number of open change streams.
- **Joining**: a subscriber added to an existing subscription is replayed changes from 1s before its `createdAt`, out of a per-collection buffer capped at 500 events and 30s that counts toward the budget and is evicted first, from the collection whose buffer holds the most. A subscriber whose replay was trimmed by age or count, or evicted for memory, before it started is disconnected, 10 at once and then one a second; the close is recorded first, so a restart still makes it.
- **Rollback** to 1.2.x replays events from each subscription's last subscribe or join, at most 2h back (API Gateway's connection limit), because the ping no longer writes the subscription; clients dedupe by `_id`. 1.2.x ignores `subscriberFanoutCloses`, so a connection still waiting for its close stays subscribed until the gateway drops it, and it ignores `subscriberFanoutTombstones`, so it only loses that filtering.

## [1.2.2] - 2026-10-27

Maintenance release: build cleanup and a dependency patch. No runtime behavior change.

### Removed
- **Dropped the dead `NPM_TOKEN` build-arg** from the `Dockerfile` (both stages) and the `README.md` / `CONTRIBUTING.md` build docs. All deps are public so it was always empty; the unused `ARG NPM_TOKEN` also tripped the BuildKit `SecretsUsedInArgOrEnv` lint. No change to the built image.

### Changed
- **Bumped `mongoose` (`7.8.9` -> `7.8.10`)** via Dependabot. Patch bump, already on main since July.

### Notes for consumers
- **Wire protocol**: unchanged from `1.2.1`.
- **Image**: same runtime contents as `1.2.1`; the only build difference is that the image config no longer carries an `ARG NPM_TOKEN` entry. Roll forward by bumping your image reference to `v1.2.2`.

## [1.2.1] - 2026-07-07

Security-only release: drains all open Dependabot alerts ahead of the public repo/package flip. No runtime behavior change.

### Security
- **Bumped `@aws-sdk/client-apigatewaymanagementapi` (`3.654.0` -> `^3.1080.0`) and `@aws-sdk/credential-providers` (`^3.665.0` -> `^3.1080.0`).** The stale, exact-pinned client dragged in vulnerable transitives; the modern SDK drops them entirely. This clears 8 of 9 alerts:
  - `fast-xml-parser` (`4.4.1`) - 1 critical, 2 high, 2 moderate, 1 low. No longer in the tree.
  - `uuid` (`9.0.1`) - 1 moderate. No longer in the tree.
  - `@smithy/config-resolver` (`3.0.13`) - 1 low. No longer in the tree.
- **Refreshed `esbuild` (`0.28.0` -> `0.28.1`)** via a lockfile update within `tsx`'s existing range (dev-only, 1 low). Clears the 9th alert.
- `pnpm audit` now reports no known vulnerabilities. None of these packages are used directly.

## [1.2.0] - 2026-07-06

First public open-source release, published to GHCR.

### Added
- **Apache-2.0 license** (`LICENSE.md` + `NOTICE`) and `CONTRIBUTING.md` - the
  repo is now open source.
- **Self-host support**: `WEBSOCKET_MANAGEMENT_ENDPOINT` env var to resolve the
  push target without SST (falls back to `Resource.websocket.managementEndpoint`
  under AWS), and `WEBSOCKET_DISABLE_CHUNKING=true` to send frames whole to a
  direct (non-API-Gateway) WebSocket gateway.
- **Public GHCR image**: released multi-arch (`linux/amd64` + `linux/arm64`) to
  `ghcr.io/bike4mind/subscriber-fanout`; the release workflow now targets GHCR.

### Changed
- **Dropped the `@bike4mind/common` dependency.** The three wire types it
  provided are now vendored in `src/wireTypes.ts`, so the repo builds standalone
  from a bare clone with no private-registry access. These imports were
  type-only and already elided from the build, so there is **no runtime change**.
- **`NPM_TOKEN` is now optional** in the Dockerfile (all deps are public).

### Notes for consumers
- **Wire protocol**: unchanged. The `data_update` / `pong` frame shapes and the
  `querySubscriptions` collection schema are structurally identical to `1.1.0`.
- **AWS/SST consumers**: point `SUBSCRIBER_FANOUT_IMAGE` at the GHCR image (ECS
  can pull the public image); no code change to your infra is required.
- **Self-host consumers**: run the image behind a WebSocket gateway that serves
  the `@connections/{id}` management shape, with the env vars documented in the
  README.

## [1.1.0] - 2026-06-03

### Added
- `README.md`: "Wire protocol contract" section explaining the post-extraction `@bike4mind/common` decoupling. Spells out which kinds of consumer-side changes need a fanout release first vs. ship immediately, and why the fanout's pinned `@bike4mind/common` version is now the source of truth for what the fanout *sends*.
- `README.md`: "WebSocket payload chunking" section documenting the `MAX_WS_PAYLOAD_BYTES = 120 * 1024` chunking path. Calls out that consumers picked this up for the first time at `v1.0.0` (their in-monorepo fanout had no chunking), and points future debuggers at this path for large-payload regressions.
- `README.md`: Release-workflow step now explicitly explains why we tag only `:vX.Y.Z` (no rolling `:main`), because the ECR is `IMMUTABLE`.

### Notes for consumers
- **Wire protocol**: unchanged (still `IDataSubscriptionUpdateAction` from `@bike4mind/common@2.96.0`).
- **Image bytes**: unchanged from `v1.0.0` modulo build timestamp. This is a docs-only release; consumers may bump from `:v1.0.0` to `:v1.1.0` at their convenience but there is no behavioral incentive to.
- **Why minor-bump and not patch**: `v1.0.x` was reserved for the initial extraction and the workflow-demo release. The documentation gap addressed here (especially the chunking call-out and the coupling-change explanation) is significant enough that I want `:v1.1.0` to be the first version consumers point at when someone asks "what does this thing actually do and how does it interact with `@bike4mind/common`."

## [1.0.1] - 2026-05-31

### Added
- `CHANGELOG.md` to track per-version changes (`docs only`; image bytes are unchanged from v1.0.0 modulo build timestamp).
- Expanded "Releasing a new version" section in `README.md` with the canonical workflow.

### Notes for consumers
- **Wire protocol**: unchanged (still `IDataSubscriptionUpdateAction` from `@bike4mind/common@2.96.0`).
- **Action required**: none. Consumers may bump from `:v1.0.0` to `:v1.0.1` whenever convenient. The actual runtime behavior is identical.
- This release exists to exercise the version-bump pipeline end-to-end so the workflow is proven before any real behavioral change ships.

## [1.0.0] - 2026-05-30

### Added
- Initial extraction from a consumer monorepo's in-tree fanout package (chunking-enabled version).
- Standalone Node 24 / pnpm 10.34.1 setup with pinned toolchain.
- `release.yml` workflow: tag-triggered, pushes `linux/amd64` image to the shared ECR (`123456789012.dkr.ecr.us-east-2.amazonaws.com/subscriber-fanout`) via OIDC (role `subscriber-fanout-publisher`).
- `ci.yml` workflow: typecheck + Docker smoke build on PR and push to main.
- `dependabot.yml` configured for security-only npm/docker/github-actions updates.
- Cross-account ECR read policy granted to the consumer dev/prod/preview accounts.

### Notes for consumers
- **Consumers**: cut over to consuming this image (replacing their local in-tree `subscriber-fanout` builds). Some pick up WebSocket payload chunking (`MAX_WS_PAYLOAD_BYTES = 120 * 1024`) for the first time; their previous fanout had no chunking. Strictly more capable for large payloads.

### Wire protocol
- `@bike4mind/common@2.96.0` types: `IDataSubscriptionUpdateAction`, `IMessageDataToClient`, `IMongoDocument`, `IQuerySubscriptionDocument`.
