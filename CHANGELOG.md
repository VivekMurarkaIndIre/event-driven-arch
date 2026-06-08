# Changelog

All notable changes to this learning repo are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] — 2026-06-08

### Added
- `campaign-fanout` project: TypeScript monorepo scaffold with strict tsconfig
- LocalStack `docker-compose.yml` running SNS, SQS, EventBridge, and DynamoDB
- `src/infra/setup.ts` provisioning all AWS resources via SDK v3 against `http://localhost:4566`
  - 4 SNS topics: `campaign-created/updated/deleted/activated`
  - 4 SQS queues each with a paired DLQ: `campaign-processor/notifier/analytics/audit`
  - 2 EventBridge buses: `campaign-bus`, `campaign-dlq-bus`
  - 1 DynamoDB table: `Campaigns` with PK/SK and `StatusCreatedAtIndex` GSI
- `npm run infra:setup` script via `tsx`
- `vitest` v4 test runner
- `/commit` Claude Code skill (`.claude/commands/commit.md`) — updates README, CHANGELOG, and resources on every commit
- `resources/` folder with architecture notes, learnings, and business context for `campaign-fanout`
