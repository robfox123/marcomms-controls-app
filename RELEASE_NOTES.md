# Release Notes

## v1.2.0 (2026-02-21)

- Updated header version label to `Marcomms Controls V 1.2`.
- Confirmed deploy chain works end-to-end:
  - GitHub -> Cloudflare Pages -> Monday Board View.
- Stabilized Cloudflare build behavior:
  - Removed `@mondaycom/apps-cli` dependency from this repo.
  - Fixed `xlsx` lockfile resolution to npm package.
  - Added `@rollup/rollup-linux-x64-gnu` optional dependency pin.

## v1.1.0 (2026-02-19 to 2026-02-21)

- Built Marcomms control panel with staged workflow:
  - Align `In Marcomms`
  - Update items to latest Programme Grid
  - Archive placeholder path
- Added progress tracking, batch updates, filters, and debug tooling.
- Added mapping verification, sheet/header matching, and manual override controls.
- Added preview and deploy flows with content-type routing.

