---
name: pomsky-deploy-dogfooding
description: "Deploy Pomsky to the dogfooding cluster. Optionally build and push a new image (as edge or a custom tag), then deploy with optional staging.yaml overrides and monitor rollout."
user-invocable: true
---

## What it does

Automates deploying Pomsky to the dogfooding cluster:

1. **(Optional) Build**: build and push a new image — either as `edge` (typically from main) or with a custom tag (e.g. `<username>-<feature>` from a feature branch)
2. **Deploy**: patch `staging.yaml` with the target image tag and any config overrides (replica counts, env vars, etc.), run `bzl run //:staging`, and monitor the Helm rollout

If the user's intent is unclear, ask whether they want to deploy the existing `edge` image or build a new one (and with what tag).

Docs:
- https://datadoghq.atlassian.net/wiki/spaces/LCP/pages/4579951384/Pomsky
- https://datadoghq.atlassian.net/wiki/spaces/LCP/pages/5457578322/Dogfooding+Setup

## Prerequisites

- `bazel-bin` must be in `PATH` (required for `bzl run`):
  ```bash
  export PATH="/path/to/pomsky/bazel-bin:$PATH"
  ```
- `crane` installed (used to resolve image digest)
- GitLab auth token available via `ddtool`

## Steps

### 1. (Optional) Build and push the image

**Skip this step if using the existing `edge` image** — CI builds it automatically on every merge to main.

To build a new image, choose a tag:
- `edge` — to rebuild from main (e.g. after a merge)
- `<username>-<feature-name>` — custom tag from a feature branch

Two build methods: **local build** (~15 min) or **CI build** (>30 min, sometimes hits 1h timeout).

#### Option A: Local build (preferred for iteration)

Run from the **pomsky repo root**:

```bash
TAG="edge"  # or "<username>-<feature-name>"
CI_JOB_TOKEN=$(ddtool auth gitlab token) ./scripts/image-tool.sh --push --sign --tag "$TAG"
```

`CI_JOB_TOKEN` is required to fetch the private PomChi dependency. Without it the build fails with `failed to get pomchi as a dependency`. Get it via `ddtool auth gitlab token` (requires prior `ddtool auth gitlab login`).

Docker must be running (`colima start`). For release builds, ensure enough resources: `colima start --cpu 8 --memory 16`.

#### Option B: CI build (tag-triggered)

CI only runs on `main`, `pomsky/staging`, or **tags** (see `.gitlab-ci.yml` workflow rules). To build from a feature branch, push a tag:

```bash
git tag -m "build for dogfooding" <username>-<feature-name>-v1 <commit-sha>
git push origin <username>-<feature-name>-v1
```

CI builds a multi-arch image (amd64 + arm64). Monitor at: `https://gitlab.ddbuild.io/DataDog/pomsky/-/pipelines?ref=<tag-name>`

CI is slower than local (>30 min, sometimes hits the 1h job timeout).

### 2. Patch staging.yaml

Update `k8s/values/staging.yaml` locally (**do not commit**):

For a custom build:
```bash
TAG="<username>-<feature-name>"
sed -i '' "s/^  tag: .*/  tag: $TAG/" k8s/values/staging.yaml
```

For `edge`, no tag change needed — just adjust replica counts or other config as needed.

### 3. (Optional) Verify the image before deploying

```bash
TAG="<username>-<feature-name>"  # or "edge"
crane manifest registry.ddbuild.io/pomsky:$TAG | jq -r '.manifests[0].digest' | xargs -I{} crane config registry.ddbuild.io/pomsky:$TAG@{} | jq .created
```

Confirm the creation timestamp matches the build you just ran.

### 4. Deploy

Before deploying, check for running workflows and last deployment time by querying deploy logs via the Datadog production MCP (search logs tool) or `pup` CLI:

```
service:helm-cnab @cnab.installation.name:"helm/v1::pomsky.logs-cloudprem.vaporeon-b.us1.staging.dog" -status:debug
```

Indexes: `build-stable`, `release-mgmt-plane`.

- If logs appear in the last 5 minutes, a deploy is in progress — wait or ask the user to cancel it in Mosaic.
- Report the timestamp of the last deploy to the user for context.

Mosaic UI: https://mosaic.us1.ddbuild.io/deployments?query=service%3Apomsky&serviceName=%2Bpomsky

```bash
TAG="<username>-<feature-name>"  # or "edge"
bzl run //:staging --define image_digest=$(crane digest registry.ddbuild.io/pomsky:$TAG)
```

If blocked by `Workflow execution is already running`, the user needs to cancel the previous workflow in Mosaic.

### 5. Monitor

Propose monitoring to the user — they can accept or skip.

#### Mosaic UI

https://mosaic.us1.ddbuild.io/deployments?query=service%3Apomsky&serviceName=%2Bpomsky

#### Deploy logs in Datadog (production)

Search Helm deploy logs using the Datadog production MCP (search logs tool) or `pup` CLI:

```
service:helm-cnab @cnab.installation.name:"helm/v1::pomsky.logs-cloudprem.vaporeon-b.us1.staging.dog" -status:debug
```

Indexes: `build-stable`, `release-mgmt-plane`.

If this query returns nothing, ask the user to grab the log explorer URL from the Mosaic deployment details page.

Key messages to watch:
- `StatefulSet is not ready: ... X out of Y expected pods` — rolling update in progress
- `Deployment is ready` — component finished rolling
- Errors/timeouts — deploy may fail if rollout takes too long

#### kubectl

```bash
# Watch pods roll
kubectl get pods -n logs-cloudprem --context vaporeon-b.us1.staging.dog -w

# Check a specific pod
kubectl describe pod <pod-name> -n logs-cloudprem --context vaporeon-b.us1.staging.dog | tail -30

# Follow logs
kubectl logs <pod-name> -n logs-cloudprem --context vaporeon-b.us1.staging.dog -c cloudprem -f

# Check image on a pod
kubectl get pod <pod-name> -n logs-cloudprem --context vaporeon-b.us1.staging.dog -o jsonpath='{.spec.containers[?(@.name=="cloudprem")].image}'
```

### 6. Restore staging.yaml (after deploy)

```bash
git checkout -- k8s/values/staging.yaml
```

## Cluster info

| Field     | Value                          |
|-----------|-------------------------------|
| Context   | `vaporeon-b.us1.staging.dog`  |
| Namespace | `logs-cloudprem`              |
| Registry  | `registry.ddbuild.io/pomsky`  |
| Dashboard | [Logs CloudPrem Dogfooding](https://ddstaging.datadoghq.com/dashboard/697-c6u-7st) (staging DD org) |

## Troubleshooting

- **`pomsky.pushed_bundle.singleexec: command not found`** → `bazel-bin` not in PATH. Run: `export PATH="$(pwd)/bazel-bin:$PATH"` from pomsky root.
- **Deploy picks up wrong tag** → Check `k8s/values/staging.yaml`; ensure it was patched before running `bzl run`.
- **Image not found** → Verify the push succeeded: `crane digest registry.ddbuild.io/pomsky:<tag>`
- **GitLab auth failure** → Re-authenticate: `ddtool auth gitlab login`, then retry.
- **`failed to get pomchi as a dependency`** → `CI_JOB_TOKEN` not set. Run: `CI_JOB_TOKEN=$(ddtool auth gitlab token) ./scripts/image-tool.sh ...`
- **Docker build OOM / exit code 101** → Colima has insufficient resources. Restart: `colima stop && colima start --cpu 8 --memory 16`
- **Stale Docker cache** → If the image digest doesn't change despite source changes, add `--no-cache` to the docker build manually.
- **Temporal "already running"** → Previous deploy workflow still active. User needs to cancel it in Mosaic, then retry.
- **Segfault (exit 139) from `bzl run`** → Known issue. The Temporal workflow is often submitted before the crash. Check Mosaic to confirm.
- **Pods crashing after deploy** → Check `kubectl describe pod` for events and `kubectl logs --previous` for crash reason. Common causes: missing env vars, bad config, volume mount issues.
- **Verify pod picked up correct config** → `kubectl exec <pod> -c cloudprem -- curl -s http://localhost:7280/api/v1/config` returns the live config.
