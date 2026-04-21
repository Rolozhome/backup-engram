#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


CORE_TABLES = ["sessions", "observations", "user_prompts", "sync_chunks"]
SYNC_TABLES_TO_RESET = ["sync_mutations", "sync_state", "sync_enrolled_projects"]
OBSERVATION_INSERT_COLUMNS = [
    "sync_id",
    "session_id",
    "type",
    "title",
    "content",
    "tool_name",
    "project",
    "scope",
    "topic_key",
    "normalized_hash",
    "revision_count",
    "duplicate_count",
    "last_seen_at",
    "created_at",
    "updated_at",
    "deleted_at",
]
PROMPT_INSERT_COLUMNS = ["sync_id", "session_id", "content", "project", "created_at"]
SESSION_INSERT_COLUMNS = ["id", "project", "directory", "started_at", "ended_at", "summary"]
SYNC_CHUNK_INSERT_COLUMNS = ["chunk_id", "imported_at"]


class MergeError(Exception):
    pass


@dataclass
class Source:
    label: str
    original: Path
    working_root: Path
    engram_dir: Path
    db_path: Path
    manifest_path: Path | None
    chunks_dir: Path | None
    temp_dir: tempfile.TemporaryDirectory[str] | None

    def cleanup(self) -> None:
        if self.temp_dir is not None:
            self.temp_dir.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect and conservatively merge two Engram backups into a new destination.",
    )
    parser.add_argument("source_a", help="First source: .tar.gz archive, .engram dir, or parent dir containing .engram")
    parser.add_argument("source_b", help="Second source: .tar.gz archive, .engram dir, or parent dir containing .engram")
    parser.add_argument(
        "--output",
        help="Create a conservative merged Engram directory at this NEW path. If omitted, only inspection/reporting runs.",
    )
    parser.add_argument(
        "--json",
        help="Optional path to write the full inspection/merge report as JSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sources: list[Source] = []
    try:
        sources = [
            resolve_source(Path(args.source_a), "source_a"),
            resolve_source(Path(args.source_b), "source_b"),
        ]
        inspection = inspect_sources(sources)
        report: dict[str, Any] = {"inspection": inspection}

        if args.output:
            merge_report = merge_sources(sources, Path(args.output), inspection)
            report["merge"] = merge_report

        print(render_report(report))

        if args.json:
            write_json(Path(args.json), report)
        elif args.output:
            write_json(Path(args.output).expanduser().resolve() / "merge-report.json", report)
    except MergeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        for source in sources:
            source.cleanup()
    return 0


def resolve_source(path: Path, label: str) -> Source:
    expanded = path.expanduser().resolve()
    if not expanded.exists():
        raise MergeError(f"{label}: path does not exist: {expanded}")

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    working_root = expanded

    if expanded.is_file():
        if not tarfile.is_tarfile(expanded):
            raise MergeError(f"{label}: file is not a readable tar archive: {expanded}")
        temp_dir = tempfile.TemporaryDirectory(prefix=f"engram-merge-{label}-")
        with tarfile.open(expanded, "r:*") as archive:
            safe_extract_tar(archive, Path(temp_dir.name))
        working_root = Path(temp_dir.name)
    elif not expanded.is_dir():
        raise MergeError(f"{label}: unsupported source type: {expanded}")

    engram_dir = locate_engram_dir(working_root)
    db_path = engram_dir / "engram.db"
    if not db_path.exists():
        raise MergeError(f"{label}: engram.db not found under {engram_dir}")

    manifest_path = engram_dir / "manifest.json"
    chunks_dir = engram_dir / "chunks"
    return Source(
        label=label,
        original=expanded,
        working_root=working_root,
        engram_dir=engram_dir,
        db_path=db_path,
        manifest_path=manifest_path if manifest_path.exists() else None,
        chunks_dir=chunks_dir if chunks_dir.is_dir() else None,
        temp_dir=temp_dir,
    )


def locate_engram_dir(root: Path) -> Path:
    if (root / "engram.db").exists():
        return root
    if (root / ".engram" / "engram.db").exists():
        return root / ".engram"
    candidates = sorted(p.parent for p in root.rglob("engram.db") if p.name == "engram.db")
    if len(candidates) == 1:
        return candidates[0]
    raise MergeError(f"Could not uniquely locate an Engram directory under {root}")


def safe_extract_tar(archive: tarfile.TarFile, destination: Path) -> None:
    destination = destination.resolve()
    for member in archive.getmembers():
        member_path = (destination / member.name).resolve()
        if not str(member_path).startswith(str(destination)):
            raise MergeError(f"Unsafe archive entry detected: {member.name}")
    archive.extractall(destination)


def inspect_sources(sources: list[Source]) -> dict[str, Any]:
    source_reports = [inspect_source(source) for source in sources]
    comparison = compare_sources(sources)
    return {"sources": source_reports, "comparison": comparison}


def inspect_source(source: Source) -> dict[str, Any]:
    with sqlite3.connect(source.db_path) as conn:
        conn.row_factory = sqlite3.Row
        tables = read_table_counts(conn)
        schemas = {table: table_columns(conn, table) for table in CORE_TABLES + SYNC_TABLES_TO_RESET if has_table(conn, table)}
        sqlite_objects = read_sqlite_objects(conn)

    manifest = read_manifest(source.manifest_path)
    chunk_files = list_chunk_files(source.chunks_dir)
    return {
        "label": source.label,
        "original": str(source.original),
        "engram_dir": str(source.engram_dir),
        "has_manifest": source.manifest_path is not None,
        "has_chunks_dir": source.chunks_dir is not None,
        "chunk_files": chunk_files,
        "manifest": manifest,
        "table_counts": tables,
        "core_table_columns": schemas,
        "sqlite_objects": sqlite_objects,
    }


def compare_sources(sources: list[Source]) -> dict[str, Any]:
    left, right = sources
    with sqlite3.connect(left.db_path) as left_conn, sqlite3.connect(right.db_path) as right_conn:
        left_conn.row_factory = sqlite3.Row
        right_conn.row_factory = sqlite3.Row
        schema_compatibility = {
            table: table_columns(left_conn, table) == table_columns(right_conn, table)
            for table in CORE_TABLES
        }
        overlaps = {
            "sessions": compare_key_sets(
                fetch_sessions(left_conn),
                fetch_sessions(right_conn),
            ),
            "observations": compare_key_sets(
                fetch_observations(left_conn),
                fetch_observations(right_conn),
            ),
            "user_prompts": compare_key_sets(
                fetch_prompts(left_conn),
                fetch_prompts(right_conn),
            ),
            "sync_chunks": compare_key_sets(
                fetch_sync_chunks(left_conn),
                fetch_sync_chunks(right_conn),
            ),
        }
    return {
        "core_schema_compatible": all(schema_compatibility.values()),
        "core_schema_by_table": schema_compatibility,
        "overlap": overlaps,
        "safe_merge_notes": [
            "sessions, observations, user_prompts, and sync_chunks can be unioned when core schemas match.",
            "FTS tables are derived indexes and should be rebuilt after inserts, not copied row-by-row.",
            "sync_mutations, sync_state, and sync_enrolled_projects are sync/replication metadata and are intentionally not merged.",
        ],
    }


def merge_sources(sources: list[Source], output_dir: Path, inspection: dict[str, Any]) -> dict[str, Any]:
    comparison = inspection["comparison"]
    if not comparison["core_schema_compatible"]:
        raise MergeError("Core table schemas differ. Inspection is available, but merge is refused.")

    output_dir = output_dir.expanduser().resolve()
    ensure_new_output_dir(output_dir)

    donor = choose_donor_source(sources)
    shutil.copy2(donor.db_path, output_dir / "engram.db")

    output_chunks = output_dir / "chunks"
    output_chunks.mkdir(parents=True, exist_ok=True)
    manifest_report = merge_chunks_and_manifest(sources, output_chunks, output_dir / "manifest.json")

    output_db = output_dir / "engram.db"
    merge_counts = merge_database(sources, donor, output_db)
    final_counts = inspect_output_counts(output_db)

    return {
        "output_dir": str(output_dir),
        "report_path": str(output_dir / "merge-report.json"),
        "donor_source": donor.label,
        "merge_counts": merge_counts,
        "final_table_counts": final_counts,
        "manifest": manifest_report,
        "warnings": [
            "Output is a conservative merged Engram directory created in a NEW location.",
            "sync_mutations, sync_state, and sync_enrolled_projects were reset instead of merged.",
            "Validate the merged directory before replacing a live ~/.engram directory.",
        ],
    }


def ensure_new_output_dir(path: Path) -> None:
    if path.exists():
        if not path.is_dir():
            raise MergeError(f"Output path exists and is not a directory: {path}")
        if any(path.iterdir()):
            raise MergeError(f"Output directory must be empty: {path}")
    else:
        path.mkdir(parents=True, exist_ok=False)


def choose_donor_source(sources: list[Source]) -> Source:
    def score(source: Source) -> tuple[int, int, int]:
        with sqlite3.connect(source.db_path) as conn:
            conn.row_factory = sqlite3.Row
            sql_text = "\n".join(row["sql"] or "" for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name"
            ))
            table_count = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchone()[0]
            observation_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        return (sql_text.count("topic_key"), table_count, observation_count)

    return max(sources, key=score)


def merge_chunks_and_manifest(sources: list[Source], output_chunks: Path, manifest_path: Path) -> dict[str, Any]:
    chunk_meta: dict[str, dict[str, Any]] = {}
    copied = 0
    for source in sources:
        manifest = read_manifest(source.manifest_path)
        for item in manifest.get("chunks", []):
            chunk_id = item.get("id")
            if chunk_id:
                chunk_meta.setdefault(chunk_id, dict(item))
        for chunk_file in iter_chunk_paths(source.chunks_dir):
            chunk_id = chunk_file.stem.split(".")[0]
            target = output_chunks / chunk_file.name
            if target.exists():
                if sha256_file(target) != sha256_file(chunk_file):
                    raise MergeError(f"Chunk collision with different content for {chunk_file.name}")
                continue
            shutil.copy2(chunk_file, target)
            copied += 1
            chunk_meta.setdefault(chunk_id, {"id": chunk_id})

    manifest = {
        "version": 1,
        "chunks": sorted(chunk_meta.values(), key=lambda item: item["id"]),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return {
        "chunk_count": len(manifest["chunks"]),
        "copied_new_files": copied,
        "manifest_path": str(manifest_path),
    }


def merge_database(sources: list[Source], donor: Source, output_db: Path) -> dict[str, Any]:
    donor_keys = read_existing_keys(output_db)
    merge_counts = {
        "sessions_inserted": 0,
        "observations_inserted": 0,
        "user_prompts_inserted": 0,
        "sync_chunks_inserted": 0,
        "sync_tables_reset": list(SYNC_TABLES_TO_RESET),
    }

    with sqlite3.connect(output_db) as out_conn:
        out_conn.row_factory = sqlite3.Row
        out_conn.execute("PRAGMA foreign_keys=OFF")
        for table in SYNC_TABLES_TO_RESET:
            if has_table(out_conn, table):
                out_conn.execute(f'DELETE FROM "{table}"')

        for source in sources:
            if source.label == donor.label:
                continue
            with sqlite3.connect(source.db_path) as in_conn:
                in_conn.row_factory = sqlite3.Row
                for row in in_conn.execute(
                    "SELECT id, project, directory, started_at, ended_at, summary FROM sessions ORDER BY started_at, id"
                ):
                    key = row["id"]
                    if key in donor_keys["sessions"]:
                        continue
                    out_conn.execute(
                        "INSERT INTO sessions (id, project, directory, started_at, ended_at, summary) VALUES (?, ?, ?, ?, ?, ?)",
                        tuple(row[column] for column in SESSION_INSERT_COLUMNS),
                    )
                    donor_keys["sessions"].add(key)
                    merge_counts["sessions_inserted"] += 1

                for row in in_conn.execute(
                    "SELECT sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at FROM observations ORDER BY created_at, updated_at"
                ):
                    key = observation_key(row)
                    if key in donor_keys["observations"]:
                        continue
                    out_conn.execute(
                        """
                        INSERT INTO observations (
                            sync_id, session_id, type, title, content, tool_name, project, scope,
                            topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at,
                            created_at, updated_at, deleted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        tuple(row[column] for column in OBSERVATION_INSERT_COLUMNS),
                    )
                    donor_keys["observations"].add(key)
                    merge_counts["observations_inserted"] += 1

                for row in in_conn.execute(
                    "SELECT sync_id, session_id, content, project, created_at FROM user_prompts ORDER BY created_at"
                ):
                    key = prompt_key(row)
                    if key in donor_keys["user_prompts"]:
                        continue
                    out_conn.execute(
                        "INSERT INTO user_prompts (sync_id, session_id, content, project, created_at) VALUES (?, ?, ?, ?, ?)",
                        tuple(row[column] for column in PROMPT_INSERT_COLUMNS),
                    )
                    donor_keys["user_prompts"].add(key)
                    merge_counts["user_prompts_inserted"] += 1

                if has_table(in_conn, "sync_chunks") and has_table(out_conn, "sync_chunks"):
                    for row in in_conn.execute("SELECT chunk_id, imported_at FROM sync_chunks ORDER BY chunk_id"):
                        key = row["chunk_id"]
                        if key in donor_keys["sync_chunks"]:
                            continue
                        out_conn.execute(
                            "INSERT INTO sync_chunks (chunk_id, imported_at) VALUES (?, ?)",
                            tuple(row[column] for column in SYNC_CHUNK_INSERT_COLUMNS),
                        )
                        donor_keys["sync_chunks"].add(key)
                        merge_counts["sync_chunks_inserted"] += 1

        rebuild_fts_if_present(out_conn, "observations_fts")
        rebuild_fts_if_present(out_conn, "prompts_fts")
        out_conn.commit()
    return merge_counts


def inspect_output_counts(db_path: Path) -> dict[str, int]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return read_table_counts(conn)


def read_existing_keys(db_path: Path) -> dict[str, set[str]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return {
            "sessions": {row["id"] for row in conn.execute("SELECT id FROM sessions")},
            "observations": {observation_key(row) for row in conn.execute(
                "SELECT sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at FROM observations"
            )},
            "user_prompts": {prompt_key(row) for row in conn.execute(
                "SELECT sync_id, session_id, content, project, created_at FROM user_prompts"
            )},
            "sync_chunks": {row["chunk_id"] for row in conn.execute("SELECT chunk_id FROM sync_chunks")}
            if has_table(conn, "sync_chunks")
            else set(),
        }


def read_table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    tables = [
        row["name"]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    return {table: conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] for table in tables}


def read_sqlite_objects(conn: sqlite3.Connection) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {"tables": [], "indexes": [], "triggers": [], "views": []}
    for object_type in result:
        result[object_type] = [
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type=? AND name NOT LIKE 'sqlite_%' ORDER BY name",
                (object_type[:-1] if object_type.endswith("s") else object_type,),
            )
        ]
    return result


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def fetch_sessions(conn: sqlite3.Connection) -> set[str]:
    return {row["id"] for row in conn.execute("SELECT id FROM sessions")}


def fetch_observations(conn: sqlite3.Connection) -> set[str]:
    return {observation_key(row) for row in conn.execute(
        "SELECT sync_id, session_id, type, title, content, tool_name, project, scope, topic_key, normalized_hash, revision_count, duplicate_count, last_seen_at, created_at, updated_at, deleted_at FROM observations"
    )}


def fetch_prompts(conn: sqlite3.Connection) -> set[str]:
    return {prompt_key(row) for row in conn.execute(
        "SELECT sync_id, session_id, content, project, created_at FROM user_prompts"
    )}


def fetch_sync_chunks(conn: sqlite3.Connection) -> set[str]:
    if not has_table(conn, "sync_chunks"):
        return set()
    return {row["chunk_id"] for row in conn.execute("SELECT chunk_id FROM sync_chunks")}


def compare_key_sets(left: set[str], right: set[str]) -> dict[str, int]:
    return {
        "shared": len(left & right),
        "left_only": len(left - right),
        "right_only": len(right - left),
    }


def observation_key(row: sqlite3.Row) -> str:
    sync_id = row["sync_id"]
    if sync_id:
        return f"sync:{sync_id}"
    return stable_hash([
        row["session_id"],
        row["type"],
        row["title"],
        row["content"],
        row["tool_name"],
        row["project"],
        row["scope"],
        row["topic_key"],
        row["normalized_hash"],
        row["revision_count"],
        row["duplicate_count"],
        row["last_seen_at"],
        row["created_at"],
        row["updated_at"],
        row["deleted_at"],
    ])


def prompt_key(row: sqlite3.Row) -> str:
    sync_id = row["sync_id"]
    if sync_id:
        return f"sync:{sync_id}"
    return stable_hash([
        row["session_id"],
        row["content"],
        row["project"],
        row["created_at"],
    ])


def stable_hash(values: Iterable[Any]) -> str:
    encoded = json.dumps(list(values), sort_keys=False, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def rebuild_fts_if_present(conn: sqlite3.Connection, table: str) -> None:
    if has_table(conn, table):
        conn.execute(f"INSERT INTO {table}({table}) VALUES ('rebuild')")


def read_manifest(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {"version": None, "chunks": []}
    return json.loads(path.read_text(encoding="utf-8"))


def list_chunk_files(chunks_dir: Path | None) -> list[str]:
    return [path.name for path in iter_chunk_paths(chunks_dir)]


def iter_chunk_paths(chunks_dir: Path | None) -> list[Path]:
    if chunks_dir is None or not chunks_dir.exists():
        return []
    return sorted(path for path in chunks_dir.iterdir() if path.is_file())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render_report(report: dict[str, Any]) -> str:
    inspection = report["inspection"]
    lines: list[str] = []
    lines.append("# Engram Merge Report")
    lines.append("")
    for source in inspection["sources"]:
        lines.append(f"## {source['label']}")
        lines.append(f"- original: {source['original']}")
        lines.append(f"- engram_dir: {source['engram_dir']}")
        lines.append(f"- manifest: {'yes' if source['has_manifest'] else 'no'}")
        lines.append(f"- chunks: {len(source['chunk_files'])}")
        lines.append("- table counts:")
        for table, count in source["table_counts"].items():
            lines.append(f"  - {table}: {count}")
        lines.append("")

    comparison = inspection["comparison"]
    lines.append("## comparison")
    lines.append(f"- core_schema_compatible: {'yes' if comparison['core_schema_compatible'] else 'no'}")
    lines.append("- overlap:")
    for table, counts in comparison["overlap"].items():
        lines.append(
            f"  - {table}: shared={counts['shared']} left_only={counts['left_only']} right_only={counts['right_only']}"
        )
    lines.append("")
    lines.append("## safe merge notes")
    for note in comparison["safe_merge_notes"]:
        lines.append(f"- {note}")

    merge = report.get("merge")
    if merge:
        lines.append("")
        lines.append("## merge")
        lines.append(f"- output_dir: {merge['output_dir']}")
        lines.append(f"- donor_source: {merge['donor_source']}")
        lines.append("- inserted rows:")
        lines.append(f"  - sessions: {merge['merge_counts']['sessions_inserted']}")
        lines.append(f"  - observations: {merge['merge_counts']['observations_inserted']}")
        lines.append(f"  - user_prompts: {merge['merge_counts']['user_prompts_inserted']}")
        lines.append(f"  - sync_chunks: {merge['merge_counts']['sync_chunks_inserted']}")
        lines.append("- final table counts:")
        for table, count in merge["final_table_counts"].items():
            lines.append(f"  - {table}: {count}")
        lines.append("- warnings:")
        for warning in merge["warnings"]:
            lines.append(f"  - {warning}")

    return "\n".join(lines)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
