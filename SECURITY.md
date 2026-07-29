# Security policy

## Supported branch

Security fixes are applied to `main`. Deploy from a reviewed commit or tag rather than an unreviewed branch.

## Sensitive material

Treat all of the following as secrets or sensitive data:

- Telegram API hash
- bot token
- Telegram `.session`, `.session-journal`, `.session-wal`, and `.session-shm` files
- `sessions/accounts.json`
- `.env` and `config.yaml`
- SQLite databases and logs
- downloaded source media
- CI artifacts containing configuration or runtime data

Runtime files are excluded by `.gitignore`, `.dockerignore`, and the packaging workflow. Configuration and account-cache writes use private file permissions where the operating system supports them.

## Reporting a vulnerability

Do not publish credentials, session files, private channel identifiers, or exploitable details in a public issue. Use a private GitHub security advisory for the repository.

Include:

- affected commit or release;
- reproduction steps using non-sensitive test data;
- expected and actual behavior;
- impact and suggested remediation.

## Credential exposure response

Deleting a secret in a later commit is not sufficient because it remains in Git history and existing clones.

1. Stop affected deployments if continued use increases exposure.
2. Rotate the bot token with BotFather.
3. revoke and recreate affected Telegram user sessions.
4. rotate the Telegram API hash when applicable.
5. replace server `.env`, `config.yaml`, and session files.
6. inspect GitHub Actions logs and artifacts.
7. remove the secret from every branch and tag history.
8. require all contributors and servers to re-clone after the rewrite.

## Removing the legacy archive from Git history

A legacy ZIP archive existed in early repository history. The current tree must not contain it, but a normal deletion cannot remove its historical blob. The later deletion commit is not evidence that clones, forks, caches, or reachable history no longer contain the archive.

History rewriting is destructive and must be coordinated during a maintenance window. Create an out-of-band mirror backup first, confirm all open branches have been merged or preserved, rotate any credentials that may have been present, and notify every collaborator. Do not run the force-push steps from an automated deployment or without explicit repository-owner approval.

```bash
# Work in a fresh mirror clone.
git clone --mirror <repository-url> migrate-cleanup.git
cd migrate-cleanup.git

# Install git-filter-repo using the package method approved for your system.
git filter-repo \
  --path Telegram-Save-Restricted-Content-main.zip.zip \
  --invert-paths \
  --force

# Verify the filename and suspicious credentials are absent before pushing.
git log --all -- Telegram-Save-Restricted-Content-main.zip.zip
git fsck --full --no-reflogs --unreachable

# Force-push only after review and explicit owner approval.
git push --force --mirror
```

After the push:

- invalidate old CI artifacts and caches;
- rotate any credentials that may have appeared inside the archive;
- re-run secret scanning across all refs;
- re-clone production and contributor working copies;
- do not merge branches created from the old history.

## Deployment hardening

The production Compose definition runs the application as a non-root user, drops Linux capabilities, enables `no-new-privileges`, and uses a read-only root filesystem. `deploy.sh` creates a consistent SQLite backup and rolls back when the replacement container does not become healthy.

Keep Docker, the host kernel, Python dependencies, and GitHub Actions dependencies updated. Review Dependabot pull requests and CI vulnerability reports before merging.
