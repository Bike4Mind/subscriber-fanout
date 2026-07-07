# subscriber-fanout

Long-running MongoDB change-stream to WebSocket fan-out service, published as a
Docker image and consumed by downstream apps (for example
[Bike4Mind](https://github.com/Bike4Mind/bike4mind)) in two modes:

- **AWS / SST** - run as an ECS service behind an API Gateway WebSocket API.
- **Self-host / Docker Compose** - run as a container behind a local WebSocket
  gateway that speaks the same API Gateway Management API shape.

See [DESIGN.md](./DESIGN.md) for the architectural rationale.

## What it does

Watches MongoDB change streams and pushes matching document changes to WebSocket
clients. A client subscribes to a Mongo query through its app's `subscribe_query`
WebSocket route (which records the subscription in the `querySubscriptions`
collection); this service watches the underlying collection's change stream and
pushes each matching change back to subscribers by `POST`ing to the WebSocket
management endpoint (the API Gateway Management API `@connections/{id}` shape).

The management endpoint is **API Gateway** in an AWS deployment, or a **local
gateway that emulates that shape** in a self-host deployment - the fanout code is
identical either way.

End goal: real-time UI updates without a page refresh, feeding an app's
offline-first sync layer (e.g. a Dexie / IndexedDB mirror of backend data).

### WebSocket payload chunking

API Gateway WebSocket has a hard 128 KB per-frame limit. Payloads above it would
silently truncate, so the fanout chunks any outgoing payload over
`MAX_WS_PAYLOAD_BYTES = 120 * 1024` into sequenced `_chunked` frames that the
client reassembles.

A **direct** WebSocket gateway (self-host) has no frame cap and forwards frames
whole, so set `WEBSOCKET_DISABLE_CHUNKING=true` there to send un-chunked frames.
(If you see large-payload regressions on an AWS consumer upgrade, the chunking
path is the first thing to check - it's the biggest behavior difference from any
pre-extraction fanout build.)

## The image

The image is published to public GHCR:

```
ghcr.io/bike4mind/subscriber-fanout:vX.Y.Z    # a released version
ghcr.io/bike4mind/subscriber-fanout:latest     # latest main
```

It is multi-arch (`linux/amd64` + `linux/arm64`) and builds with no registry
auth - every dependency is public and the wire types are vendored in
`src/wireTypes.ts` (see [Wire contract](#wire-contract)), so a bare clone builds
and runs standalone.

### Consuming it - AWS / SST

Point an `sst.aws.Service` at the image; the image reference comes from an env/
repo variable so a fork can publish and point at its own registry:

```ts
new sst.aws.Service('subscriberFanout', {
  cluster,
  link: [secrets.MONGODB_URI, websocketApi],
  image: process.env.SUBSCRIBER_FANOUT_IMAGE, // e.g. ghcr.io/bike4mind/subscriber-fanout:vX.Y.Z
  cpu: '0.25 vCPU',
  memory: '2 GB',
  permissions: [{ actions: ['execute-api:ManageConnections'], resources: ['*'] }],
});
```

`MONGODB_URI` and the WebSocket management endpoint are provided by the SST link
layer as `Resource.*` (see [Runtime configuration](#runtime-configuration)).

### Consuming it - self-host / Docker Compose

Add the fanout as a service alongside Mongo and a WebSocket gateway that serves
the `@connections/{id}` management shape. Two flows:

**Pull the published image** (default):

```yaml
services:
  subscriber-fanout:
    image: ghcr.io/bike4mind/subscriber-fanout:latest
    environment:
      MONGODB_URI: mongodb://mongo:27017/bike4mind
      STAGE: selfhost
      WEBSOCKET_MANAGEMENT_ENDPOINT: http://ws:3001
      WEBSOCKET_DISABLE_CHUNKING: 'true'
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: minioadmin      # dummy creds; the local gateway ignores the SigV4 signature
      AWS_SECRET_ACCESS_KEY: minioadmin
```

**Build from source** instead of pulling (e.g. running local edits): build the
image and tag it with the name the stack expects, then `up` uses the local
image and won't try to pull:

```sh
docker build -t ghcr.io/bike4mind/subscriber-fanout:latest .
docker compose -f compose.selfhost.yaml --env-file .env.selfhost up -d
```

## Runtime configuration

The fanout resolves each setting from an explicit env var first (self-host /
plain Docker), falling back to an SST-linked `Resource` when the env var is
absent (AWS). Set the env vars for self-host; leave them unset under SST.

| Setting | Env var (self-host) | SST fallback (AWS) |
|---|---|---|
| Mongo connection string (supports `%STAGE%`) | `MONGODB_URI` | `Resource.MONGODB_URI.value` |
| Stage name (substitution + logging) | `STAGE` | `Resource.App.stage` |
| WebSocket management endpoint to push to | `WEBSOCKET_MANAGEMENT_ENDPOINT` | `Resource.websocket.managementEndpoint` |
| Send frames whole (no 128 KB chunking) | `WEBSOCKET_DISABLE_CHUNKING=true` | - (chunking on) |

AWS credentials are resolved from the standard chain (ECS -> instance -> env ->
shared file). In self-host, dummy `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
satisfy the SDK; the local gateway ignores the SigV4 signature. Under SST, the
ECS task role needs `execute-api:ManageConnections`.

## Wire contract

This service is standalone: the types it needs are vendored in
[`src/wireTypes.ts`](./src/wireTypes.ts), so there is no shared-package
dependency. Two contracts must stay structurally compatible with the consumer:

1. **The `querySubscriptions` collection** - written by the consumer's
   `subscribe_query` handler, read by this service. Its schema mirrors
   [`src/querySubscription.ts`](./src/querySubscription.ts).
2. **The frames this service emits** - `data_update`
   (`IDataSubscriptionUpdateAction`) and `pong`, defined in `src/wireTypes.ts`.
   The consumer's client parses these.

Compatibility rules:

- **Backward-compatible change** (adding an optional field, broadening a union):
  safe; ship either side first.
- **Breaking change** (renaming/removing a field, narrowing a type): ship the
  fanout release first, then the consumers; document it in the `CHANGELOG.md`
  "Notes for consumers" block.

## Releasing a new version

1. Land behavioral changes on `main` via PR. CI typechecks and smoke-builds the
   image (no push).
2. Open a release PR that bumps `version` in `package.json` (SemVer: patch for
   bugfix, minor for feature, major for a breaking wire change) and adds a
   `CHANGELOG.md` entry with a "Notes for consumers" block.
3. After it merges, tag and push:
   ```sh
   git checkout main && git pull
   git tag vX.Y.Z && git push origin vX.Y.Z
   ```
4. The [Release workflow](./.github/workflows/release.yml) builds a multi-arch
   image and pushes `:vX.Y.Z` (and `:latest`) to GHCR.
5. Each consumer bumps its image reference (the AWS `SUBSCRIBER_FANOUT_IMAGE`
   variable, or the self-host compose tag) and redeploys. Consumers roll forward
   independently.

## Local development

```sh
pnpm install       # no registry auth needed; all deps are public
pnpm typecheck
pnpm build
pnpm dev           # tsx watch; requires MONGODB_URI + STAGE in the environment
```

Build the Docker image (all deps are public, so no registry auth is needed):

```sh
docker build -t ghcr.io/bike4mind/subscriber-fanout:local .
```

## Repo conventions

- Node 24, pnpm 10.34.1 (pinned via `packageManager` + Dockerfile).
- Dependabot: security-only (scheduled bumps disabled).
- CI: typecheck + Docker smoke build on PRs and pushes to `main`.
- Releases: tag-triggered; `v*` tags publish a multi-arch image to GHCR.

## License

[Apache-2.0](./LICENSE.md). See [CONTRIBUTING.md](./CONTRIBUTING.md) to get
started.
