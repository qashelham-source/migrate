# Cleanup Auditor 2.0

Cleanup Auditor 2.0 expands temporary-file deletion diagnostics without changing cleanup timing.

## Audit events

- `JOB_DIR_CREATED` records each migration job directory when it is created.
- `TEMP_CLEANUP_START` records the path, file count, and total bytes before deletion starts.
- `TEMP_CLEANUP_DELETED` confirms the directory was removed.
- `TEMP_CLEANUP_INCOMPLETE` records remaining file count, bytes, and up to 20 remaining file names with sizes.
- `TEMP_CLEANUP_FAILED` records the exception and remaining files when deletion raises an error.
- `TEMP_CLEANUP_NOT_FOUND` records cleanup attempts for an already missing directory.

This release does not delete old directories automatically and does not change upload, verification, retry, or queue behavior.
