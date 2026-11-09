"""Search the analytics zone for anything that looks like a direct identifier.

Defence in depth: the policy should already prevent leaks; this checks that it did. Findings
report table, column, pattern and count, never the matching values themselves.

    python -m src.leak_scan
"""

from __future__ import annotations

import os
import sys

# PostgreSQL regular expressions
PATTERNS = {
    "ssn": r"[0-9]{3}-[0-9]{2}-[0-9]{4}",
    "phone": r"\(?[0-9]{3}\)?[ .-][0-9]{3}-[0-9]{4}",
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    "mrn": r"MRN-[0-9]{6,}",
}


def text_columns(conn) -> list[tuple[str, str]]:
    return conn.execute(
        """select table_name, column_name from information_schema.columns
           where table_schema = current_schema()
             and data_type in ('text', 'character varying', 'character', 'jsonb')
             and table_name in (select table_name from information_schema.tables
                                where table_schema = current_schema() and table_type = 'BASE TABLE')
           order by 1, 2"""
    ).fetchall()


def scan(conn) -> list[dict]:
    findings = []
    for table, column in text_columns(conn):
        for name, pattern in PATTERNS.items():
            count = conn.execute(
                f'select count(*) from "{table}" where "{column}"::text ~ %s', (pattern,)
            ).fetchone()[0]
            if count:
                findings.append({"table": table, "column": column, "pattern": name, "rows": count})
    return findings


def main() -> None:
    import psycopg

    with psycopg.connect(os.environ["ANALYTICS_DATABASE_URL"]) as conn:
        findings = scan(conn)
    if findings:
        for f in findings:
            where = f"{f['table']}.{f['column']}"
            print(f"POSSIBLE PHI: {where} matches {f['pattern']} in {f['rows']} rows")
        sys.exit(1)
    print("leak scan: no identifier patterns found in the analytics zone")


if __name__ == "__main__":
    main()
