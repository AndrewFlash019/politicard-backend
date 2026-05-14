# Security TODOs

## Make this repo private — DO MANUALLY

The Supabase DB password, service-role key, and several API keys were
historically committed to this repo while it was public. History has been
partially scrubbed via `git filter-repo` and force-push, but secrets that
were ever public must be treated as permanently compromised.

**Action — manually:** go to
<https://github.com/AndrewFlash019/politicard-backend/settings>, scroll to
**Danger Zone → Change repository visibility → Make private**, then confirm
by typing the repo name.

Audit forks first (private conversion does not affect existing forks):
<https://github.com/AndrewFlash019/politicard-backend/network/members>

## Open rotation work (as of 2026-05-14)

All credentials below were exposed in commit `a6dbb76` (initial) and/or
`2d4446c`, on a public repository. Even after history scrub + force-push,
GitHub's commit cache may retain old SHAs for ~90 days, and any pre-existing
clone or fork still has the values. **Treat all of these as compromised
and rotate.**

- [ ] **Supabase DB password** (`REDACTED-OLD-DB-PASSWORD-PLACEHOLDER`) — Dashboard → Project
      Settings → Database → Reset database password. Then update
      `DATABASE_URL` in local `.env` AND on deploy host.
- [ ] **`SUPABASE_SERVICE_KEY`** — Dashboard → Project Settings → API →
      roll service_role key. Worst leak (bypasses RLS).
- [ ] **`GOOGLE_AI_STUDIO_API_KEY`** — <https://aistudio.google.com/apikey>
- [ ] **`GOOGLE_CIVIC_API_KEY`** / **`CIVIC_API_KEY`** — Google Cloud Console
- [ ] **`CONGRESS_API_KEY`** — <https://api.congress.gov/sign-up/>
- [ ] **`OPENSTATES_API_KEY`** — <https://openstates.org/accounts/profile/>
- [x] **`SECRET_KEY`** — rotated locally 2026-05-14 (still needs deploy update;
      rotating will log out all users on next deploy restart)

## Pre-commit hook installed

`.githooks/pre-commit` blocks `.env`, `env`, key/cert files, and content
matching known API-key / DB-URL / private-key patterns. Activate on each
clone with `git config core.hooksPath .githooks`. See `.githooks/README.md`.

## Remaining history exposure (second scrub needed)

`git filter-repo --path .env --invert-paths` removed only the dot-prefixed
file. Two other files in history still leak the DB password:

- `env` (no dot, repo root) — full mirror of `.env`
- `alembic.ini` line 89 — `sqlalchemy.url` had a hardcoded DB password (since redacted in history; working-tree value now blank and loaded from `DATABASE_URL` env var by `alembic/env.py`)

Recommended fix:

1. Rotate DB password first (above) so the leaked value is dead.
2. Edit `alembic.ini` line 89 to use env var, e.g. leave URL blank and
   load `DATABASE_URL` from env in `alembic/env.py`.
3. Run a second filter-repo pass:
   ```bash
   git filter-repo --path env --invert-paths --force
   git filter-repo --replace-text <(echo 'REDACTED-OLD-DB-PASSWORD-PLACEHOLDER==>***REDACTED***') --force
   ```
4. Force-push all branches again.
