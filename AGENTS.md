# Working in KingyoOrchestrator

- This is a public repository. Keep private handoffs, real infrastructure
  identifiers, credential files, and machine-specific paths out of tracked files,
  issues, and pull requests. Use synthetic examples in tests and documentation.
- Local handoffs under `kingyo-handoff/` and `KUMOSQL-ACCESS-HANDOFF.md` are ignored.
  Consult them locally when relevant; do not force-add or reproduce them publicly.
- Python 3.11+ uses a `src/` package layout. Keep orchestration decisions separate
  from cloud adapters and persistence as those modules are introduced.
- The starter is observe-only. Do not add implicit cloud calls, execution,
  credential inspection, or default-project discovery to bootstrap commands.
- For cloud work, pass the project explicitly, prefer metadata and dry runs,
  and get user authorization before writes, paid queries, executions, or IAM changes.
- Keep offline tests independent of cloud credentials and external services.
- Run `python -m ruff check .`, `python -m ruff format --check .`, and the relevant
  `python -m pytest` tests after code changes. Check packaging when it changes.
- Update README and architecture notes when behavior or setup instructions change.

## Pull request reviews and merges

- Claude is responsible for merging reviewed PRs unless the user assigns the merge
  to another agent.
- Agents may share the repository owner's GitHub account. When reviewer and author
  have the same GitHub login, submit a `COMMENT` review instead of `APPROVE`.
  GitHub forbids self-approval; do not retry it or ask for another identity solely
  to obtain a formal approval.
- A recorded review comment is sufficient for this workflow. After actionable
  findings are resolved, required CI checks pass, and GitHub reports the PR as
  mergeable, Claude may merge it without another person's approval. A missing
  formal approval alone is not a blocker when GitHub does not require one.
- Follow any explicit user hold or enforced repository rule that requires approval.
