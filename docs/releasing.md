# Releasing

The publishable npm package is `effect-view-server`. Workspace packages under
`@effect-view-server/*` are internal implementation packages and must stay
private.

Releases use Changesets for versioning and npm Trusted Publishing for the
actual publish. Do not add an `NPM_TOKEN`; the release workflow uses GitHub OIDC
with `id-token: write`.

## One-time npm setup

Before the first publish, configure npm Trusted Publishing for the
`effect-view-server` package:

- Package name: `effect-view-server`
- Repository: `bmvantunes/effect-view-server`
- Workflow file: `release.yml`
- Allowed action: `npm stage publish`
- Environment: leave unset unless the workflow is also changed to use a GitHub
  environment

The public package manifest uses npm's normalized repository URL
`git+https://github.com/bmvantunes/effect-view-server.git`; keep that aligned
with the npm trusted publisher repository instead of using an SSH URL. The
publish job installs a known npm CLI version because trusted publishing requires
modern npm OIDC support.

The package already sets `publishConfig.provenance: true`, and the release
workflow has `id-token: write`, so npm can attach provenance to staged
versions. Staged publishing requires the package to already exist on npm; this
repository was bootstrapped with a one-time manual `effect-view-server@0.0.1`
publish before stage-only automation was enabled.

## Contributor flow

For any PR that should release a new package version, add a changeset:

```sh
vp run -w changeset
```

Choose the release type for `effect-view-server`:

- `patch`: bug fixes and documentation-safe package changes
- `minor`: new backwards-compatible API or runtime features
- `major`: breaking public API/runtime behavior

Internal `@effect-view-server/*` packages are not published. If a change only
touches tests, CI, docs, benchmarks, or private internals and should not publish
an npm version, do not add a versioned changeset. Add an empty changeset only
when a validation step asks for explicit no-release intent.

## Main branch flow

On every push to `main`, `.github/workflows/release.yml` runs:

1. `vp install --frozen-lockfile`
2. browser dependency installation
3. `vp run -w ready`
4. Changesets action

If unreleased changesets exist, the action opens or updates a `Version packages`
PR. When that PR is merged, the same workflow builds `effect-view-server` and
stages a sanitized npm artifact through trusted publishing. A maintainer must
then approve the staged package with `npm stage approve <stage-id>` to publish
it publicly, or reject it with `npm stage reject <stage-id>`. After approval,
manually run the `Release` workflow on `main` with the approved version as the
workflow input; the release script observes that the exact version is now public
and creates the public
`effect-view-server@<version>` git tag. The staged artifact intentionally
excludes source maps, source-map references, scripts, dev dependencies, internal
`@effect-view-server/*` workspace metadata, and internal workspace import
specifiers. The publish script skips `effect-view-server@0.0.0`, so enabling
this workflow cannot accidentally publish the placeholder development version.
The staging job may push an `effect-view-server@<version>-staged` marker tag as
a best-effort signal that approval is pending. It is not authoritative: reruns
still ask npm so rejected stages can be restaged and approved stages can be
converted into public release tags. The public
`effect-view-server@<version>` release tag is only created after npm reports
that the version is actually published.

## Manual checks

Useful local checks before merging release-sensitive changes:

```sh
vp run -w ready
vp run effect-view-server#build
vp run effect-view-server#test
```

For PRs that should publish a package version, also verify the changeset state:

```sh
vp exec changeset status --since main
```

Use the heavier capacity gate only when promoting production-like runtime
readiness:

```sh
vp run -w release-candidate:capacity
```
