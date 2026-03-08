"""Tool for inspecting the WIT V1 SQLite database."""
import sqlite3
from pathlib import Path
from crewai.tools import BaseTool

ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = ROOT / "data" / "db" / "wit_database.db"

SAFE_QUERIES = (
    "SELECT", "PRAGMA", "EXPLAIN", "WITH"
)


class DBInspectorTool(BaseTool):
    """Run read-only SQL queries on wit_database.db."""

    name: str = "db_inspector"
    description: str = (
        "Run a read-only SQL query on wit_database.db. "
        "Only SELECT, PRAGMA, EXPLAIN and WITH queries are allowed. "
        "Input: a valid SQLite SQL query. "
        "Returns query results as formatted text."
    )

    def _run(self, query: str) -> str:
        query = query.strip()
        upper = query.upper().lstrip()
        if not any(upper.startswith(kw) for kw in SAFE_QUERIES):
            return "ERROR: Only read-only queries allowed (SELECT, PRAGMA, EXPLAIN, WITH)."
        try:
            con = sqlite3.connect(str(DB_PATH))
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            cur.execute(query)
            rows = cur.fetchmany(100)
            con.close()
            if not rows:
                return "Query returned no results."
            headers = list(rows[0].keys())
            lines = [" | ".join(headers)]
            lines.append("-" * len(lines[0]))
            for row in rows:
                lines.append(" | ".join(str(row[h]) for h in headers))
            return "\n".join(lines)
        except Exception as e:
            return f"ERROR executing query: {e}"


class DBSchemaInspectorTool(BaseTool):
    """Inspect the schema of wit_database.db tables."""

    name: str = "db_schema_inspector"
    description: str = (
        "Get the schema (columns, types, constraints) of a table in wit_database.db. "
        "Input: table name (e.g. 'smart_wallets', 'token_analytics'). "
        "Returns CREATE TABLE statement and row count."
    )

    def _run(self, table_name: str) -> str:
        table_name = table_name.strip()
        try:
            con = sqlite3.connect(str(DB_PATH))
            cur = con.cursor()
            cur.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            row = cur.fetchone()
            if not row:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [r[0] for r in cur.fetchall()]
                con.close()
                return f"Table '{table_name}' not found. Available tables: {', '.join(tables)}"
            schema = row[0]
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            con.close()
            return f"{schema}\n\nRow count: {count}"
        except Exception as e:
            return f"ERROR: {e}"
