# Engram backup inspect/merge utility

Minimal, conservative tooling for comparing two Engram backups and optionally building a NEW merged `.engram`-style directory without touching either input.

## What this is for

- **Restore**: replace one machine's `.engram/` with a single backup.
- **Merge**: inspect two sources and build a separate destination so you can review the result before using it anywhere.

This repo is intentionally aimed at the second case.

## Schema findings

The inspected backups expose these important SQLite tables:

- `sessions` — session metadata, safely unionable by `id`
- `observations` — persistent memories/notes, safely unionable by `sync_id` or a content fingerprint fallback
- `user_prompts` — saved prompts, safely unionable by `sync_id` or a content fingerprint fallback
- `sync_chunks` — imported chunk ids, safely unionable by `chunk_id`
- `observations_fts` / `prompts_fts` plus triggers — derived full-text indexes; these should be rebuilt, not merged row-by-row
- `sync_mutations`, `sync_state`, `sync_enrolled_projects` — sync/replication metadata; these are NOT safely mergeable without deeper product guarantees

So the safe line is clear: merge durable content, rebuild FTS, and do **not** pretend sync state can be merged blindly.

## Usage

### Inspect only

```bash
python3 engram_merge.py backup-a.tar.gz backup-b.tar.gz
```

You can also point at extracted directories:

```bash
python3 engram_merge.py /path/to/extracted/.engram ~/.engram
```

Optional JSON report:

```bash
python3 engram_merge.py backup-a.tar.gz ~/.engram --json ./report.json
```

### Conservative merge into a NEW output directory

```bash
python3 engram_merge.py backup-a.tar.gz ~/.engram --output ./merged-engram
```

That creates:

- `merged-engram/engram.db`
- `merged-engram/manifest.json`
- `merged-engram/chunks/`
- `merged-engram/merge-report.json`

The script refuses to write into a non-empty directory.

## Safe workflow

1. **Inspect first** and review table counts + overlap.
2. **Merge into a NEW directory** only, never into a live `~/.engram`.
3. Review the merged report and, if you want, inspect the SQLite file manually.
4. Back up the destination machine's current `~/.engram` before doing anything else.
5. Only then decide whether to replace the destination with the merged directory.

## Limitations

- This is a **conservative** merge, not an authoritative Engram sync engine.
- `sync_mutations`, `sync_state`, and `sync_enrolled_projects` are reset in merged output instead of being merged.
- If two chunk files share the same name but not the same bytes, merge aborts.
- Merge is refused when the core table schemas (`sessions`, `observations`, `user_prompts`, `sync_chunks`) differ.
- Inputs are never modified.

## Why not just overwrite files?

Because that's how you lose data.

Two different `.engram` directories may each contain durable content plus sync metadata and chunk references. Blind overwrite preserves one side and destroys the other. This utility gives you an inspect-first path and a conservative union where that union is actually defensible.
