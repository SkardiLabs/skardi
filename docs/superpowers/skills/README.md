# Shared agent skills

Reusable Claude Code skills maintained with the repo. `.claude/` is
personal workspace configuration and stays untracked — install a shared
skill into your own setup with a symlink (one-time, per clone):

```bash
mkdir -p .claude/skills
ln -s ../../docs/superpowers/skills/source-pack .claude/skills/source-pack
```

The skill then auto-triggers in your Claude Code sessions (or invoke it
explicitly, e.g. `/source-pack`), and `git pull` keeps it current since
the link points into the repo.

| Skill | Purpose |
| --- | --- |
| [source-pack](source-pack/SKILL.md) | Develop a new Open Connector source pack end-to-end: live contract reconciliation, implementation under the admission gate, self-review against the repo's review standards, PR submission. |
| [graph-source](graph-source/SKILL.md) | Connect a property graph (Cypher over AGE) to Skardi end-to-end: least-privilege provisioning, `type: graph` declaration and views, registration-state verification, correct query patterns, pipeline parameterization, troubleshooting. |
