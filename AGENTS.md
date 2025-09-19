# Agent Operations Manual

## 1. Purpose & Scope

- Provide a single, current reference for anyone (human or AI) working in this repository.
- Eliminate ambiguity by defining how work is planned, executed, documented, and verified.

## 2. Actors & Responsibility

- **User**: Defines requirements, prioritises work, approves changes, and ultimately owns all outcomes.
- **AI Agent**: Executes only the work the User authorises and keeps all artefacts aligned with this manual.

## 3. Core Operating Principles

1. **User-Directed Tasks**: Treat the User's request as the source of truth. Capture additional context only when the User asks for it.
2. **Scope Confirmation**: Clarify uncertainties before modifying code. If requirements shift mid-task, confirm the new scope with the User.
3. **Traceability**: Reference the current request in code comments, commits, and conversation summaries so decisions stay attributable to the User.
4. **Minimal Bureaucracy**: Backlog and task files are optional. Maintain or create documentation only when the User explicitly requests it.
5. **Single Source of Truth**: Keep information in one place. If you create notes or docs, link to them instead of duplicating.
6. **Controlled File Creation**: Create new files (including documentation) only when the User explicitly approves the specific file.
7. **Named Constants**: Replace repeated literal values with descriptive constants in generated code.
8. **Sense Check Data**: Validate data, requirements, and results for consistency before acting on them.

## 4. Workflow & Scope Control

- Start every change discussion by confirming you understand the User's request.
- Keep the conversation focused on the agreed scope; call out scope creep as soon as you see it.
- If you discover adjacent issues, surface them as optional follow-up ideas rather than expanding the active task.
- When access or tooling limitations appear, pause and ask the User how to proceed instead of guessing.

## 5. Work Tracking

- Dedicated backlog/task artifacts are not required for day-to-day work.
- When the User asks for work tracking, follow their preferred format (e.g., ad-hoc checklist, markdown file, issue link).
- If historical docs already exist for a feature, update them only when you touch the feature and the User confirms they still matter.

## 6. Testing Strategy

- Apply risk-based testing and follow the test pyramid: unit at the base, integration for cross-component behaviour, E2E for critical flows.
- Provide a lightweight test plan in your response when implementing code. Scale detail with risk.
- Prefer automated tests; document any manual verification you perform or that remains outstanding.

## 7. Change Management

- Mention the current task/request ID (if one exists) in commits and pull requests; otherwise describe the user-visible change.
- Summaries should highlight impacted components, testing performed, and follow-up considerations.
- Do not revert or modify unrelated in-flight work without explicit User approval.

---
This manual replaces prior `.cursorrules` and `claude.md` content; refer to `AGENTS.md` as the authoritative guide going forward.
