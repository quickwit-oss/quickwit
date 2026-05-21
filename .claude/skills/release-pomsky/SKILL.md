---
name: release-pomsky
description: "Release a new Pomsky version. Runs the full 3-step release process with preflight checks and confirmation between each step: deploy to release cluster, bump version, update Helm charts."
user-invocable: true
---

## What it does

Guides you step-by-step through the full Pomsky release process. Each step is explained before it runs, and you are asked to confirm before proceeding to the next one.

1. **Preflight** — verify repo is on `main`, clean, and in sync with remote
2. **Step 1: Deploy to release cluster** — build and push the `release`-tagged image, deploy to `pomsky-release`, monitor for regressions
3. **Step 2: Bump Pomsky version** — run the release script, get the PR merged as a merge commit, verify Docker Hub
4. **Step 3: Update Helm charts** — follow the Helm chart release process

## Preflight checks

Before doing anything, run all three checks and stop if any fail:

```bash
# 1. Must be on main
git branch --show-current   # expected: "main"

# 2. Working tree must be clean — no staged, unstaged, or untracked changes
git status --porcelain       # expected: no output

# 3. Must be in sync with remote
git fetch origin
git status -sb               # expected: "## main...origin/main" with no ahead/behind indicator
```

If any check fails, explain what needs fixing and stop. Do not proceed until all three pass.

## Step 1 — Deploy to release cluster

**Explain to the user before starting:**
> This step builds a signed Docker image tagged `release` (~15 min), pushes it to the internal registry, and deploys it to the dedicated `pomsky-release` cluster — a smaller sibling of dogfooding that receives a sample of production logs. The cluster must run for at least a few hours before proceeding so we can confirm there are no regressions.

**Ask the user to confirm before running any commands.**

### 1a. Authenticate with GitLab

```bash
ddtool auth gitlab login
```

### 1b. Build, push, and sign the release image

Run from the **pomsky repo root**:

```bash
CI_JOB_TOKEN=$(ddtool auth gitlab token) ./scripts/image-tool.sh --push --sign --tag release
```

`CI_JOB_TOKEN` is required to fetch the private PomChi dependency — without it the build fails with `failed to get pomchi as a dependency`. Docker must be running (`colima start`). For resource-intensive builds: `colima start --cpu 8 --memory 16`.

### 1c. Deploy to the release cluster

```bash
bzl run //:staging-release --define image_digest=$(crane digest registry.ddbuild.io/pomsky:release)
```

### 1d. Verify the deployed image

After the deploy, confirm the pods are running the correct image and show the user its build timestamp:

```bash
# Check the image digest on a running pod
kubectl get pods -n logs-cloudprem --context vaporeon-b.us1.staging.dog | grep pomsky-release | awk '{print $1}' | head -1 | xargs -I{} kubectl get pod {} -n logs-cloudprem --context vaporeon-b.us1.staging.dog -o jsonpath='{.spec.containers[?(@.name=="cloudprem")].image}'

# Check the build timestamp
crane config "registry.ddbuild.io/pomsky:release" --platform linux/arm64 | jq '.created'
```

Report both the digest and the build timestamp to the user. The timestamp should match today's build — if it's from a previous day, the cache was reused and the image is stale.

When reporting how long ago the image was built, always run `date -u` first to get the current UTC time, then compute the difference against the UTC timestamp from `crane config`. Do not estimate from local time or log timestamps — they are in the user's local timezone and will give a wrong result.

### 1e. Monitor for regressions

Share these links with the user and ask them to monitor the cluster for at least a few hours:

- [Dashboard](https://ddstaging.datadoghq.com/dashboard/697-c6u-7st/logs-cloud-prem-dogfooding?tpl_var_cloudprem_cluster_id%5B0%5D=logs-cloudprem-pomsky-release&tpl_var_kube_app_instance%5B0%5D=pomsky-release&tpl_var_orgstore_cluster%5B0%5D=pomsky-release-metastore&live=true)
- [Logs](https://ddstaging.datadoghq.com/logs?query=service%3Apomsky%20kube_app_instance%3Apomsky-release&live=true)

**Important:** Make sure the image deployed to the cluster is the most recent release image before moving on.

**Ask the user to confirm the cluster looks healthy before proceeding to Step 2.**

## Step 2 — Bump Pomsky version

**Explain to the user before starting:**
> This step runs the release script, which creates a PR bumping Cargo.toml versions across the workspace and placing a git tag on the release commit. The PR must be merged as a **merge commit** — squashing or rebasing would detach the tag from main, breaking CI's image build.

**Ask the user to confirm before running any commands.**

### 2a. Run the release script

```bash
./scripts/release_pomsky.sh
```

This creates a PR with:
- Cargo.toml version bumps across the workspace
- A git tag pointing at the release commit

### 2b. Merge the PR

- Open the PR URL printed by the script
- Request review and get approval
- ⚠️ **Merge as a merge commit** — NOT squash, NOT rebase
- Confirm CI builds the image: check [Docker Hub](https://hub.docker.com/r/datadog/cloudprem/tags)

**Ask the user to confirm the PR is merged and the Docker image is visible on Docker Hub before proceeding to Step 3.**

## Step 3 — Update Helm charts

**Explain to the user before starting:**
> This final step updates the Helm chart to reference the new Pomsky image version, making the release available for production deployments.

**Ask the user to confirm before proceeding.**

Follow the [Helm chart release process](https://datadoghq.atlassian.net/wiki/spaces/LCP/pages/4928145536/Helm+chart#Release-process).

**Ask the user to confirm the Helm chart has been updated before declaring the release complete.**

## Completion

Once all three steps are confirmed, announce the release is complete and remind the user to verify the image running on the release cluster is the final release image (not a candidate).

## Troubleshooting

- **`failed to get pomchi as a dependency`** → `CI_JOB_TOKEN` not set or expired. Re-run `ddtool auth gitlab login`, then retry.
- **Docker build OOM / exit code 101** → Colima has insufficient resources. Restart: `colima stop && colima start --cpu 8 --memory 16`
- **`quickwit-cli` link fails with `signal: 9, SIGKILL` / `cannot allocate memory`** → Docker Desktop has insufficient memory. The release build with LTO + full feature set needs 12–16 GB. Go to **Docker Desktop → Settings → Resources**, set Memory to at least 12 GB, then click **Apply & Restart**. If the build output is truncated and the error isn't obvious, always check the last few lines — OOM kills show up as `ResourceExhausted: process did not complete successfully: cannot allocate memory`.
- **`bzl run` / `pomsky.pushed_bundle.singleexec: command not found`** → `bazel-bin` not in PATH. Run: `export PATH="$(pwd)/bazel-bin:$PATH"` from the pomsky root.
- **Tag missing from main after merge** → PR was squash- or rebase-merged. The tag is now orphaned. Consult the team — the tag may need to be re-placed on the merge commit manually.
- **Image not appearing on Docker Hub** → Check the CI pipeline for the release tag on GitLab.
