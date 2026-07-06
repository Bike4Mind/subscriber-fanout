# Changelog

All notable changes to this image will be documented here.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this image follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

When you release a new version, append a section here and bump `version` in `package.json` before tagging. Consumers read this file to decide whether the new version is safe to roll.

## [Unreleased]

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
