---
name: spec-new
description: Scaffold a new spec file in /specs from the standard template. Use when adding a feature that needs a spec before planning begins.
---

Create a new spec file in `/specs/` following the project's spec template.

The user will provide: the spec number (e.g. `15`), the title, and a brief description
of what the feature does. If not provided, ask before proceeding.

Use this exact template:

```markdown
---
id: "<number>"
title: <Title>
status: todo
phase: <phase number>
depends_on: [<comma-separated list of spec ids this depends on>]
requirements: [<FR/NFR IDs from planning/01-requirements.md>]
---

# Spec <number> · <Title>

## What
<1-3 sentences: what this spec adds to the system>

## Why
<1-3 sentences: why this is needed, what invariant or capability it unlocks>

## What to build
<Bullet list of classes/interfaces to create or modify, grouped by module>

## Acceptance criteria
<Numbered list of concrete, testable done-conditions. Each criterion must be
verifiable by running a test or command — not just "it works".>

## Out of scope
<Explicit list of things that are NOT in this spec, to prevent scope creep>
```

Rules:
- Keep it high-level (WHAT and WHY, not line-level HOW).
- Every acceptance criterion must be testable.
- Out-of-scope items must be explicit.
- Do NOT start writing implementation code — this skill produces the spec only.
- After creating the file, remind the user to run `/spec-plan <number>` next.
