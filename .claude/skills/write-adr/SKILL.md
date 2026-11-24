---
name: write-adr
description: Write or update an Architecture Decision Record for Mahlaba Rush in docs/adr/. Use when the user asks to write an ADR, document a design decision or trade-off, or fill in one of the ADRs listed in SPEC §14.
---

# Write an ADR

ADRs live in `docs/adr/NNN-kebab-case-title.md`. The list of ADRs to write is in
`assets/doc/SPEC.md` §14 — reuse its number and title when the decision is one of those.

## Steps

1. Read the SPEC sections that motivate the decision (search `assets/doc/SPEC.md` for
   "trade-off", "Why", and the §14 row). Quote section numbers, don't copy whole tables.
2. Check `docs/adr/` for an existing file with the same number; update it instead of duplicating.
3. Write the file using the template below. Keep it under ~1 page.
4. If the ADR changes something the SPEC states, say so in the reply — don't silently edit the SPEC.

## Template

```markdown
# ADR-NNN: <Title>

- **Status:** Proposed | Accepted | Superseded by ADR-XXX
- **Date:** YYYY-MM-DD
- **SPEC refs:** §x.y, §z

## Context
What problem forces a decision. Mention the constraint (e.g. per-order ordering, laptop resources).

## Decision
One or two sentences, active voice: "We use …".

## Alternatives considered
| Option | Why not (here) |
|--------|----------------|

## Consequences
- Good: …
- Bad / cost: …
- Interview point: one line an engineer could say out loud.

## Mahlaba analogy
One line tying it to the shop (see SPEC §18 glossary).
```
