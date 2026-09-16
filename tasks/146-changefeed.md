# Model-level changefeed update filtering

Parent: https://github.com/PlaceOS/local/issues/146
Agent/task: codex-146-pgorm-20260916-root
Branch: ai/146-model-changefeed
Start: 2026-09-16

## Contract and scope

Expose `changefeed_ignore_updates :last_seen, :current_item_id` on a model. Pass its configured columns through Model.changes, Database.listen_change_feed and ChangeFeedHandler.add_listener to EventBus.ensure_cdc_for. Default models pass nil, preserving installed table policy. Explicit declarations are validated against persisted attributes, mapped to database column names if supported by the ORM, and isolated between siblings. Specify/test inheritance and override behavior. Do not silently reset policies or add a global list. Shared-table policy conflicts must propagate and a failed subscription must leave no orphan registration that prevents retry.

Use EventBus 1.1.0 (version bump on upstream main 3f116fa) and confirm shard resolver availability. No changes to PlaceOS models until user releases pg-orm.

## Checklist

- [x] Read instructions, latest coordination comments and native blockers; claim isolated branch/worktree.
- [ ] Add failing regression specs before implementation.
- [ ] Resolve EventBus 1.1.0 and declare compatible minimum dependency.
- [ ] Implement per-model declaration, validation and listener propagation.
- [ ] Prove default behavior, ignored/mixed updates, inserts/deletes, model isolation/inheritance, conflicts and retry cleanup.
- [ ] Run one spec suite at a time via agent; fix existing failures; format and lint.
- [ ] Independent review and GitHub CI; commit/squash merge and release claim.
- [ ] Handoff to user for pg-orm release before models work.

## Review / verification

Pending. Use real PostgreSQL CDC integration specs plus compile-time declaration validation where appropriate. Preserve existing API compatibility and model changefeed behavior. Record results in PR and parent issue.
