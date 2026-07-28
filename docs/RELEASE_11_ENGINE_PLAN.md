# Release 11.0 — Durable Migration Engine

This branch is the staged implementation branch for the next engine release. It must not be deployed to production until every phase and fault-injection gate below passes.

## Delivery rule

Development is incremental on this branch, but production delivery remains one final Release 11.0 merge.

## Phase 1 — Durable state and schema

- Add the durable engine state vocabulary without breaking legacy queue reads.
- Add additive, idempotent schema migrations.
- Add immutable migration plans and plan items.
- Add job-attempt history.
- Add job leases, lease tokens, expiry, and heartbeat fields.
- Add compare-and-swap transition helpers.
- Preserve existing production behavior behind a compatibility layer.

Exit gates:

- Existing databases upgrade repeatedly without data loss.
- Existing queue rows remain readable.
- Invalid state transitions are rejected.
- Two workers cannot hold the same valid lease.
- An expired lease can be recovered safely.

## Phase 2 — Lease-aware scheduler and recovery

- Replace fetch-then-update claiming with atomic lease acquisition.
- Add heartbeat renewal and graceful lease release.
- Classify interrupted download and upload work.
- Route ambiguous upload states to reconciliation rather than direct retry.

Exit gates:

- Forced process termination does not lose a job.
- A stale worker cannot update a job after losing its lease.
- Restart does not blindly requeue an ambiguous upload.

## Phase 3 — Album aggregate and manifests

- Treat each Telegram media group as one aggregate job.
- Add album quiet-window planning.
- Persist ordered members, caption owner, expected count, and manifest identity.
- Validate all downloaded members before publication.

Exit gates:

- No partial album can reach committed state.
- Album ordering remains stable across restart.
- Missing members remain waiting instead of publishing an incomplete group.

## Phase 4 — Publish intent and reconciliation

- Persist publish intent before every Telegram send operation.
- Persist destination acknowledgements immediately after Telegram returns.
- Add UPLOADED_UNCONFIRMED and NEEDS_RECONCILIATION handling.
- Probe destination evidence before any ambiguous retry.

Exit gates:

- Termination after Telegram acknowledgement but before normal completion does not cause automatic duplicate publication.
- Reconciliation can accept a confidently matched destination result.
- Uncertain cases enter quarantine.

## Phase 5 — Verification, commit, and cleanup

- Separate acknowledgement, structural verification, and content-confidence verification.
- Commit only verified jobs.
- Delete temporary data only after verified commit.
- Add a database-aware stale-directory reaper.

Exit gates:

- Verified jobs clean up completely.
- Unverified and ambiguous jobs retain only the data required by policy.
- Reaper never deletes data owned by an active lease or unresolved publish intent.

## Phase 6 — Storage reservations and destination lanes

- Reserve estimated bytes before starting downloads.
- Add pressure, critical, and emergency modes.
- Add ordered destination lanes and destination-specific pause/FloodWait state.
- Keep upload serialization per writer while allowing independent operations to continue.

Exit gates:

- Concurrent downloads cannot overcommit disk through stale free-space observations.
- One paused destination does not block healthy destinations.
- Storage pressure stops new downloads while allowing upload, verification, and cleanup progress.

## Phase 7 — Fault-injection and compatibility gates

Required simulations:

- terminate during download
- terminate after download completion
- terminate during upload
- terminate after Telegram acknowledgement and before normal database completion
- terminate during verification
- database locked
- disk nearly full
- download FloodWait
- upload FloodWait
- destination permission loss
- missing album member
- duplicate worker claim
- configuration change during an active job

Final release requirements:

- no duplicate publication in covered scenarios
- no partial committed album
- no lost job
- no unbounded temporary-storage leak
- restart resumes safely
- legacy queue data remains supported or is migrated reversibly

## Excluded from Release 11.0

- dashboard redesign
- cosmetic UI work
- AI Error Doctor changes
- multi-server distributed workers
- unrelated admin commands

## Merge policy

The branch remains unmerged until all seven phases pass CI and the final migration/recovery review is complete.
