# Git hooks

Tracked hooks that block commits containing `.env`, `env`, key/cert files,
or content matching known API-key / DB-URL / private-key patterns.

## Setup (run once per clone)

```bash
git config core.hooksPath .githooks
```

That's it — `.githooks/pre-commit` will run before every commit and reject
anything matching `pre-commit`'s rules. Bypass (only when you've verified
no secret leaked) with `git commit --no-verify`.

## What it blocks

- Files: `.env`, `env`, `.env.*`, `*.env`, `*.pem`, `*.key`, `*.pfx`,
  `*.p12`, `id_rsa`, `id_ed25519`, `credentials*.json`,
  `firebase-adminsdk*.json`, `*secrets*.yml|yaml`
- Patterns in added content: Anthropic / Google / Stripe / AWS / GitHub /
  Slack / Supabase keys, JWTs, postgres connection strings with embedded
  credentials, PEM private-key headers
