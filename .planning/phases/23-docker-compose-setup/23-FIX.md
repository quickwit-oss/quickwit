---
phase: 23-docker-compose-setup
plan: 23-FIX
type: fix
wave: 1
depends_on: []
files_modified: [quickwit/Makefile]
autonomous: true
---

<objective>
Fix 1 UAT issue from phase 23.

Source: 23-UAT.md
Diagnosed: yes - root cause identified
Priority: 0 blocker, 1 major, 0 minor, 0 cosmetic
</objective>

<execution_context>
@~/.claude/get-shit-done/workflows/execute-plan.md
@~/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/ROADMAP.md

**Issues being fixed:**
@.planning/phases/23-docker-compose-setup/23-UAT.md

**Original plan for reference:**
@.planning/phases/23-docker-compose-setup/23-01-PLAN.md

**Makefile to modify:**
@quickwit/Makefile
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix UAT-001 - docker-metrics-up --wait flag error</name>
  <files>quickwit/Makefile</files>
  <action>
**Root Cause:** The `--wait` flag in `docker-compose up` waits for ALL containers to be "running", but minio-init is a one-shot init container that intentionally exits (with code 0) after creating the bucket. Docker Compose treats any container exit as a failure when using --wait.

**Issue:** "minio-init container exits (0) after creating bucket, but --wait flag treats any exit as failure. Make reports Error 1 even though postgres and minio are healthy."

**Expected:** `make docker-metrics-up` completes without error, Minio and Postgres are healthy.

**Fix:** Modify the `docker-metrics-up` target to:
1. Start containers without `--wait`
2. Explicitly wait only for `minio` and `postgres` services to be healthy using `docker-compose up -d minio postgres --wait`
3. Then start the init container separately (it will run and exit)

Replace:
```makefile
docker-metrics-up:
	docker-compose --profile postgres --profile metrics-e2e up -d --wait
```

With:
```makefile
docker-metrics-up:
	docker-compose --profile postgres --profile metrics-e2e up -d minio postgres --wait
	docker-compose --profile metrics-e2e up -d minio-init
```

This starts minio and postgres first with --wait (they stay running), then starts minio-init separately (its exit won't cause an error since we don't wait for it).
  </action>
  <verify>
- `make docker-metrics-up` completes with exit code 0
- `docker ps` shows minio and postgres running
- `curl -f http://localhost:9000/minio/health/live` returns 200
- `docker exec postgres pg_isready` returns success
- Test bucket exists (check via minio console or mc)
  </verify>
  <done>UAT-001 resolved - docker-metrics-up completes without error, minio-init exit no longer causes failure</done>
</task>

</tasks>

<verification>
Before declaring plan complete:
- [ ] `make docker-metrics-up` exits with code 0
- [ ] Minio service healthy and accessible
- [ ] Postgres service healthy and accessible
- [ ] Test bucket created (minio-init ran successfully)
- [ ] `make docker-metrics-down` still works correctly
</verification>

<success_criteria>
- UAT-001 from 23-UAT.md addressed
- `make docker-metrics-up` works without errors
- Ready for re-verification with /gsd:verify-work 23
</success_criteria>

<output>
After completion, create `.planning/phases/23-docker-compose-setup/23-FIX-SUMMARY.md`
</output>
