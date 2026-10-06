# Defense in Depth

Tracking against https://github.com/jaredwray/agentic/blob/main/skills/security/defense-in-depth-nodejs/SKILL.md.

Profile: npm library · public

## 1. Security docs
- [x] `SECURITY.md` present — contact info + "How this repository is secured" summary — PR #105
- [x] `DEFENSE_IN_DEPTH.md` present (this file) — PR #105

## 2. CODEOWNERS and cloud bootstrap
- [x] `.github/CODEOWNERS` covers `/.github/`, `/.vscode/`, `/.cursor/`, `/.devcontainer/`, `/.claude/`, `/.codex/`, `/scripts/` with owners the maintainer names — PR #177
- [x] Codespaces, Cursor Cloud Agents, and Claude Code on the web bootstrap Aikido Safe Chain via scripts/setup-cloud-environment.sh (--ci shims, frozen lockfile); Claude Code runs it from `.claude/hooks/session-start.sh` (web sessions only, install log on stderr, 600s timeout) and `.gitignore` keeps `.claude/settings.json` and `.claude/hooks/` tracked — PR #178
- [ ] Codex cloud environments use Manual setup with `bash ./scripts/setup-cloud-environment.sh` as the setup and maintenance script (manual)
- [ ] Claude Code on the web environments allow `malware-list.aikido.dev` (Custom network access plus the default package-manager list) (manual)
- [x] Dev Container `image` pinned by digest (`name:<tag>@sha256:<digest>`; not a floating tag) — PR #171

## 3. Dependencies (pnpm)
- [x] `packageManager: pnpm@11.3+` pinned in `package.json` — PR #169
- [x] 7-day cooldown: `minimumReleaseAge: 10080`, `minimumReleaseAgeStrict: true`, `minimumReleaseAgeIgnoreMissingTime: false`; no first-party `minimumReleaseAgeExclude` — PR #108
- [x] `trustPolicy: no-downgrade`; no first-party `trustPolicyExclude` — verified 2026-10-06
- [x] Lifecycle scripts blocked: `strictDepBuilds: true`, `dangerouslyAllowAllBuilds: false`, `allowBuilds: {}` baseline — PR #109
- [x] `blockExoticSubdeps: true` — PR #110
- [x] Lockfile committed; CI installs with `pnpm install --frozen-lockfile` — PR #111
- [x] No `.github/dependabot.yml`; other dependency-update tools (if any) open PRs only — never auto-merge — verified 2026-10-06

## 4. GitHub Actions
- [x] `permissions: contents: read` (or `{}` + per-job grants) on every workflow — verified 2026-10-06
- [x] No `contents: write` except jobs whose purpose is mutating the repo (GitHub Release, Changesets version PR); generated output is a workflow artifact, never committed back from CI — verified 2026-10-06
- [x] Every action pinned to a full commit SHA (`npx actions-up`) — PR #170
- [x] Every job installs Socket Firewall (`SocketDev/action` SHA-pinned, `firewall-version` pinned); `pnpm install` / `npm install` run as `sfw pnpm install` / `sfw npm install` — verified 2026-10-06
- [x] `.github/workflows/check-workflows.yaml` lints workflows with zizmor on every PR — PR #116
- [ ] Workflow `name:` and job `name:` contain no spaces (kebab-case) so they can be set as required status checks (PR #179 pending)
- [x] `persist-credentials: false` on checkouts that don't push — PR #116
- [x] No `pull_request_target` on workflows that run untrusted PR code — verified 2026-10-06
- [x] Artifact-publishing workflows disable `actions/setup-node` default caching (`package-manager-cache: false`) to prevent cache poisoning — PR #116
- [x] No npm tokens (or other registry credentials) in Actions secrets — verified 2026-10-06

## 5. npm publishing — npm libraries only
- [x] OIDC trusted publishing configured **stage-only** on npmjs.com for the publish workflow — it can stage, never publish live (manual) — maintainer 2026-08-18
- [x] `.github/workflows/release.yaml` packs then stages with `pnpm stage publish ./packed/*.tgz --no-git-checks` — PR #117
- [x] Maintainer promotes staged versions with 2FA (manual) — maintainer 2026-08-18
- [x] Drydock connected — staged releases reviewed before promotion (manual) — maintainer 2026-08-18
- [x] No direct publish rights: package requires 2FA and disallows tokens (manual) — maintainer 2026-08-18
- [x] `package.json` `repository.url` accurate so provenance maps to this repo — verified 2026-10-06

## 6. Security tooling
- [x] Aikido runs on every build — verified 2026-10-06
- [x] Aikido release gate: the release workflow's stage-publish job `needs:` a passing `scan-release` — PR #118
- [x] Socket reviews every PR that changes dependencies — verified 2026-10-06

## 7. Repository lockdown
- [x] Phishing-resistant 2FA (passkeys / hardware keys) on the GitHub and npm accounts (manual) — maintainer 2026-08-18
- [x] Recovery codes stored offline in a password manager (manual) — maintainer 2026-08-18
- [ ] `lockdown-repo.sh` applied by a repo admin (never committed to this repo); `--check` with `--required-checks` and `--allowed-actions` passes (PRs required on the default branch, merges blocked unless required status checks pass, tag ruleset, immutable releases, fork-PR approval (public repos), read-only workflow tokens, Actions allowlist, secret scanning, Dependabot disabled, private vulnerability reporting (public repos))
