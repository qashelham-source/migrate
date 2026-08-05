---
name: Queue advancement — verification_pending_jobs
description: Why multi-source queues can stall on source 1 forever, and the correct fix.
---

## The rule

`verification_pending_jobs` must NOT be a hard retry-blocker in `_source_outcome`. It must be folded into `review_items` instead.

**Why:** The Verifier runs *before* `_source_outcome` is called. If it cannot create `verification_results` rows (network error on individual jobs, or `dest_message_ids IS NULL` so `fetch_for_verification` returns 0 rows while `source_work_state` still counts all copied jobs as pending), returning `CycleOutcome("retry", 60)` creates an infinite loop: cycle restarts from source 1, worker finds 0 pending, verifier fails again, sources 2+ are never reached.

**How to apply:** When `verification_pending_jobs > 0` and all other states are terminal, treat them as `review_items` and return `CycleOutcome("complete")`. The user sees them in Issue Center. Same rule applies in `_write_initial_wait_status` (which calls `_source_outcome`) — already-complete sources must be skipped on restart.

## Companion display bugs fixed at the same time

- `source_complete` phase was grouped with `scanning`/`uploading` under "⚡ Running — source N/T". It now has its own headline: "✅ Source N/T done — moving to source N+1".
- Waiting sources (2, 3…) were invisible in the dashboard list until they had DB records. Fixed by comparing `source_total` from `status.json` against `len(source_progress)` and appending "📋 +N more source(s) waiting in queue".
