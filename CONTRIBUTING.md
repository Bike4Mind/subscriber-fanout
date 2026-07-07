# Contributing to subscriber-fanout

Thanks for your interest in contributing! `subscriber-fanout` is a small, focused
service: it watches MongoDB change streams and fans matching document changes out
to WebSocket clients. It is published as a Docker image and consumed by downstream
apps (for example [Bike4Mind](https://github.com/Bike4Mind/bike4mind)) via SST.

The scope is intentionally narrow, so most contributions are bug fixes, robustness
improvements, docs, and tests rather than large features. This guide covers how to
get a change from idea to merged PR.

## Table of Contents

- [License and contributions](#license-and-contributions)
- [Code of Conduct](#code-of-conduct)
- [Ways to contribute](#ways-to-contribute)
- [Before you write code: issue-first for anything non-trivial](#before-you-write-code-issue-first-for-anything-non-trivial)
- [Development setup](#development-setup)
- [Project layout](#project-layout)
- [Engineering standards](#engineering-standards)
- [Git workflow: fork, branch, PR](#git-workflow-fork-branch-pr)
- [CI: the checks your PR must pass](#ci-the-checks-your-pr-must-pass)
- [Reporting security vulnerabilities](#reporting-security-vulnerabilities)
- [Reporting bugs](#reporting-bugs)

## License and contributions

This project is licensed under the [Apache License 2.0](./LICENSE.md). **No CLA is
required.** Under Apache-2.0 Section 5, any contribution you intentionally submit
for inclusion in the work is licensed under the same Apache-2.0 terms, unless you
explicitly state otherwise. In short: you keep your copyright, and by opening a PR
you agree your contribution is offered under Apache-2.0.

Please only submit work that is yours to license (or that you are authorized to
contribute on behalf of your employer).

## Code of Conduct

Be respectful, be constructive, assume good faith, and keep discussions focused on
the work. Harassment or abuse of any kind is not tolerated. Report unacceptable
behavior to the maintainers.

## Ways to contribute

- **Bug reports** - a precise, reproducible report is one of the most valuable
  contributions there is. See [Reporting bugs](#reporting-bugs).
- **Bug fixes** - especially around change-stream resumption, reconnection, and
  large-payload chunking.
- **Docs** - improvements to `README.md`, `DESIGN.md`, and `CHANGELOG.md`.
- **Tests** - this service is under-tested; coverage is welcome.

## Before you write code: issue-first for anything non-trivial

For anything larger than a trivial fix (typo, obvious one-line bug, doc
correction):

1. **Search existing issues** to see if it's already tracked.
2. **Open an issue first** describing the problem and your proposed approach.
3. **Wait for a maintainer to confirm direction** before investing significant
   effort.

Because this service sits on a shared wire contract (the `data_update` / `pong`
frame shapes vendored in `src/wireTypes.ts`, and the `querySubscriptions`
collection schema), changes that alter behavior can affect every consumer of the
image. Discuss those up front.

> **Keep the schema in sync.** `src/querySubscription.ts` is intentionally a
> byte-for-byte mirror of the consumer-side `QuerySubscriptionModel`. If you change
> one, change both - the file carries a "keep in sync" header for this reason.

## Development setup

### Prerequisites

- **Node.js 24.x** (see `.nvmrc` - `nvm use`)
- **pnpm 10.34.1** (`npm install -g pnpm@10.34.1`, or via `corepack`)
- **Docker** (only needed to build/run the image)

### Install, typecheck, build

```bash
pnpm install --frozen-lockfile
pnpm typecheck      # tsc --noEmit
pnpm build          # tsc -> dist/
pnpm dev            # tsx watch (requires MONGODB_URI + STAGE in the environment)
```

All dependencies are public, so `pnpm install` needs no registry auth.

### Docker build

```bash
docker buildx build --platform linux/amd64 -t subscriber-fanout:local .
```

See [README.md](./README.md) for how the image is built, released, and consumed
(including outside AWS / in a self-host stack).

## Project layout

```
src/
  index.ts              # SubscriberFanout: change-stream lifecycle + fan-out loop
  querySubscription.ts  # Mongoose model (mirror of the consumer-side model)
  websocket.ts          # send-to-connection + 120KB payload chunking + creds chain
types/sst.d.ts          # SST Resource stubs so the repo typechecks standalone
```

## Engineering standards

- **TypeScript**: avoid `any`; prefer `unknown` + narrowing, generics, or unions.
  If `any` is truly unavoidable, add a comment justifying it. `pnpm typecheck` must
  pass.
- **Comments**: keep the WHY, gotchas, invariants, and cross-file "keep in sync"
  notes. Skip decorative or restated-code comments. Plain ASCII in comments.
- **Behavior changes need tests**; bug-fix PRs should include a test that fails
  without the fix.
- **Wire-compatibility**: adding an optional field is backward compatible; renaming
  or removing a field is a breaking wire change - call it out in the PR and in the
  `CHANGELOG.md` "Notes for consumers" block.

## Git workflow: fork, branch, PR

1. **Fork** the repository and clone your fork.
2. **Create a topic branch off `main`**: `type/short-description` - e.g.
   `fix/resume-token-expiry`, `docs/self-host-consumption`.
3. **Make focused commits** in [Conventional Commits](https://www.conventionalcommits.org/)
   format: `type(scope): description` - e.g. `fix(fanout): clear resume token on oplog gap`.
4. **One concern per PR.** Small, focused PRs merge fast.
5. **Never commit secrets** (tokens, connection strings with credentials).
6. **Update `CHANGELOG.md`** for any behavior or wire change, and bump `version` in
   `package.json` if you're cutting a release (see the README "Releasing a new
   version" section).
7. **Push to your fork** and open a PR against `main`.

PRs are **squash-merged**; the PR title becomes the commit message, so it must be a
valid conventional-commit header. Allowed types: `feat | fix | docs | style |
refactor | perf | test | build | ci | chore`.

## CI: the checks your PR must pass

CI (`.github/workflows/ci.yml`) runs on every PR and push to `main`:

| Check | What it verifies | Run it locally |
|---|---|---|
| **Typecheck** | `tsc --noEmit` across the project | `pnpm typecheck` |
| **Build** | `tsc` compiles to `dist/` | `pnpm build` |
| **Docker build (smoke)** | The image builds `linux/amd64` (no push) | `docker buildx build ...` (above) |

Run typecheck and build locally before pushing.

## Reporting security vulnerabilities

**Do not open a public issue for a security vulnerability.** Use GitHub's private
vulnerability reporting on this repository ("Security" tab -> "Report a
vulnerability"). We'll coordinate a fix and disclosure with you.

## Reporting bugs

Open an issue and include:

- **Environment**: how the image is deployed (AWS/SST, self-host compose, other),
  and the image tag.
- **Steps to reproduce**: numbered, minimal, deterministic.
- **Expected vs. actual behavior**, with logs/screenshots (redact any secrets or
  connection strings).

Thanks for contributing!
