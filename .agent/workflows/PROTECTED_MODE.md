---
description: STRICT SAFETY PROTOCOLS - READ BEFORE ANY ACTION
---

# 🛑 PROTECTED MODE: ARCHITECTURE OVERHAUL

**STATUS**: ACTIVE
**BRANCH**: refactor/architecture-overhaul

## 🚫 ABSOLUTE PROHIBITIONS

1.  **NO PUSH TO MAIN**: 
    - You are strictly FORBIDDEN from running `git push origin main` or `git push origin master`.
    - All work must be done on `refactor/architecture-overhaul` or other feature branches.

2.  **NO MERGING TO MAIN**:
    - Do not merge `refactor/architecture-overhaul` into `main`.
    - Do not open a Pull Request to `main` unless explicitly instructed to "Deploy to Production".

3.  **IMMUTABLE FILES**:
    - **FILE**: `app/api/v1/endpoints/integration.py`
    - **RULE**: DO NOT MODIFY. This file powers the active Power BI integration and is critically stable.
    - **EXCEPTION**: None.

4.  **DATA PRESERVATION**:
    - **RULE**: NEVER delete existing GitHub Releases or Tags.
    - **REASON**: These serve as the production database.

## ✅ PERMITTED ACTIONS

- You MAY push to `origin refactor/architecture-overhaul`.
- You MAY create new files, delete other files, and refactor the entire system on the feature branch.
- You MAY create new GitHub Releases with *new* names (e.g., `alpha-v2`), but do not touch `dataset-latest` or `sync-*`.

## 🔒 ENFORCEMENT

If a user request seems to conflict with these rules, **STOP** and cite this policy.
