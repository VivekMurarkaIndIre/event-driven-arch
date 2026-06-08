You are helping the user commit changes to their event-driven-design learning monorepo. The repo always works on `main`. Follow every step below in order — do not skip any.

---

## Step 1 — Understand what changed

Run `git diff HEAD --name-only 2>/dev/null || git status --short` to get the list of modified/new files. Read the diff for any changed source files so you can write accurate descriptions. Capture the project(s) affected (e.g. `campaign-fanout`).

---

## Step 2 — Update root README.md

Read the current `/Users/vivekmurarka/github/Learning/event-driven-design/README.md` if it exists.

Update (or create) it with the following sections. Do NOT remove sections that already exist — only update or append.

### Required sections

**# event-driven-design**
One-sentence description of the repo purpose: "A learning monorepo exploring event-driven architecture patterns in TypeScript."

**## Projects**
A table of every top-level folder that contains a `package.json`:
| Project | Description |
|---------|-------------|

**## Quick Start**
```bash
# Start infrastructure
docker compose up -d

# Install deps (run inside each project)
npm install
```

**## Commands — \<project-name\>**
One section per project. Scan each project's `package.json` scripts block and `docker-compose.yml` (if present) and list every command verbatim:

```bash
npm run <script>    # description from the script value
docker compose up -d
docker compose down
```

Include the `infra:setup` script and any test or build commands found.

**## Resources**
```
resources/                        Architecture notes and learnings
resources/campaign-fanout/        Notes specific to the campaign-fanout project
```

---

## Step 3 — Update CHANGELOG.md

Read the current `/Users/vivekmurarka/github/Learning/event-driven-design/CHANGELOG.md` if it exists.

Prepend a new entry in **Keep a Changelog** format using today's date. Derive the content from the git diff and any $ARGUMENTS the user passed.

```markdown
## [Unreleased] — YYYY-MM-DD

### Added
- …

### Changed
- …

### Fixed
- …
```

Omit sections that have no entries. If CHANGELOG.md does not exist, create it with a header:

```markdown
# Changelog

All notable changes to this learning repo are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---
```

followed by the new entry.

---

## Step 4 — Update resources/

Check which project(s) were touched in Step 1. For each affected project, open its corresponding resources file(s) and **append** new notes — never overwrite existing content.

### resources/README.md
Keep an up-to-date index table:
| File | Contents |
|------|----------|

### resources/campaign-fanout/architecture.md
Append a dated section if architecture files changed:
```markdown
### YYYY-MM-DD — <short title>
**Decision:** …
**Why:** …
**Trade-offs:** …
```

### resources/campaign-fanout/learnings.md
Append a dated bullet group when new patterns, gotchas, or insights are visible in the diff:
```markdown
### YYYY-MM-DD
- …
```

### resources/campaign-fanout/business-context.md
Update only when the business problem, data model, or flow changes materially.

---

## Step 5 — Stage and commit

Stage all modified files:
```bash
git add README.md CHANGELOG.md resources/ <all other modified files>
```

Write a commit message:
- Subject line: imperative mood, ≤72 chars, conventional-commit prefix (`feat:`, `chore:`, `docs:`, `fix:`)
- Body: 2–4 bullet points describing *why*, not *what*
- Trailer: `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`

Commit with:
```bash
git commit -m "$(cat <<'EOF'
<subject>

<body>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

Never use `--no-verify`. If a hook fails, investigate and fix before retrying.

After the commit, print:
```
✓ Committed: <subject line>
  Files: <count> changed
  Branch: main
```

---

## Notes for edge cases

- If the user passed text via $ARGUMENTS, treat it as the commit message subject (still generate the body from the diff).
- If there is nothing staged or modified, say so and exit without committing.
- If a file being updated does not exist yet, create it from the templates above.
