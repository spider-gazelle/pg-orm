# Explicit database-only changefeed columns

Parent: https://github.com/PlaceOS/local/issues/146
Owner: codex-146-metadata-20260916-root

- [ ] Add regression for generated database-only column changes while unrelated source columns still notify.
- [ ] Extend changefeed_ignore_updates with database_columns: [:column], retaining positional persisted-attribute validation, symbol syntax and id rejection.
- [ ] Preserve inheritance/override/empty/fresh-array behavior and runtime database column existence validation.
- [ ] Format/lint, focused and full CI, independent review, squash merge; release gates final models adoption.
