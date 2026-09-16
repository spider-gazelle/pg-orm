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
- [x] Add failing regression specs before implementation.
- [x] Verify EventBus 1.1.0 exact commit and declare minimum dependency; public version resolution still awaits tag/commit-pin choice.
- [x] Implement per-model declaration, validation and listener propagation.
- [x] Prove default behavior, ignored/mixed updates, inserts/deletes, model isolation/inheritance, conflicts and retry cleanup.
- [x] Run one spec suite at a time via agent; full suite 483 examples, 0 failures/errors; format and lint pass.
- [ ] Independent review and GitHub CI; commit/squash merge and release claim.
- [ ] Handoff to user for pg-orm release before models work.

## Review / verification

Local verification: `PGORM_ENV=local GITHUB_ACTION=true ./test` passed 483 examples, 0 failures/errors (15.02 seconds runtime), using a temporary Dockerfile/override pinned to EventBus 3f116fa (declares 1.1.0). Temporary files are removed after verification; the final dependency is the version constraint awaiting a tag. Ameba 1.6.4 on Crystal 1.16.3 passes (48 files); host format and diff checks pass. Eleven new specs cover runtime CDC policy and isolated compiler validation/inheritance.

Independent review approved after fixing registry-name collisions with injective escaping. Abstract inheritance fixtures run in isolated compiler processes to avoid unrelated existing virtual base-class dispatch limitations in the full suite. No active-model changes.

Release blocker: upstream has no EventBus tags, so Shards cannot resolve `~> 1.1.0`. Asked user whether to create the v1.1.0 tag, wait for their tag, or use an exact commit pin. Do not merge or release pg-orm before resolving this dependency. Models remains unchanged.
