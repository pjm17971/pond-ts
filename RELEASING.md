# Releasing

`pond-ts` publishes to npm from GitHub Actions when a `v*` tag is pushed.

## Release flow

1. Merge the release-ready changes into `main`.
2. Bump the version:

```sh
npm version patch
```

Use `minor` or `major` instead when appropriate.

3. Push the commit:

```sh
git push
```

4. Push the version tag:

```sh
git push --tags
```

Pushing the tag triggers the publish workflow in `.github/workflows/release.yml`.

## Notes

- `npm version` updates `package.json`, updates `package-lock.json`, creates a git commit, and creates a matching git tag such as `v0.1.5`.
- The repo CI should be green before cutting a release.
- If a publish fails after the tag is created, fix the issue on `main`, bump to the next version, and push a new tag instead of trying to reuse the old one.
