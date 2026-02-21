# Deployment Workflow (GitHub -> Cloudflare Pages -> Monday)

This project is deployed from GitHub to Cloudflare Pages, then loaded in Monday Board View using an external hosting URL.

## Production URL

- `https://marcomms-controls-app.pages.dev`

## Standard workflow

1. Make local code changes.
2. Commit and push to `main`.
3. Cloudflare Pages auto-builds from GitHub.
4. Monday Board View loads the updated production URL.

No tunnel is required for normal production use.

## Local release commands

```bash
git add .
git commit -m "Describe change"
git push
```

## Cloudflare Pages settings

- Framework preset: `None`
- Build command: `npm run build`
- Build output directory: `build`
- Production branch: `main`

## Monday Board View settings

- Hosting type: `External hosting`
- URL: `https://marcomms-controls-app.pages.dev`

## Versioning checklist

1. Update `APP_VERSION` in `src/App.tsx` (UI label).
2. Update `version` in `package.json` (semver).
3. Run `npm run build`.
4. Commit and push.
5. Confirm Cloudflare deploy and Monday view update.

## Known build fixes already applied

1. Removed `@mondaycom/apps-cli` from this deployable web app repo (Pages install conflict).
2. `xlsx` now resolves from npm (`0.18.5`) in both `package.json` and `package-lock.json`.
3. Added optional dependency pin:
   - `@rollup/rollup-linux-x64-gnu: 4.36.0`
   - This avoids Cloudflare/npm optional dependency misses for Rollup.

## If Cloudflare fails

1. Confirm deployment uses latest commit SHA from `main`.
2. Retry from latest commit (not an old failed deployment snapshot).
3. If lockfile corruption is suspected:

```bash
rm -f package-lock.json
npm install --package-lock-only --ignore-scripts
git add package-lock.json
git commit -m "Regenerate lockfile"
git push
```
