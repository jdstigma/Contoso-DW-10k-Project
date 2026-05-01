"""
db_doc.py — SQLite Database Documentation Generator
Usage: python3 db_doc.py contoso_v2_10k.db

Outputs a clean schema report to the terminal and saves it as db_doc.txt
"""

import sqlite3
import sys
import os
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH   = sys.argv[1] if len(sys.argv) > 1 else 'contoso_v2_10k.db'
OUT_PATH  = 'db_doc.txt'
COL_WIDTH = 100  # total line width for dividers

# ── Helpers ───────────────────────────────────────────────────────────────────
def divider(char='─', width=COL_WIDTH):
    return char * width

def section(title):
    pad = (COL_WIDTH - len(title) - 2) // 2
    return f"\n{'═' * pad} {title} {'═' * (COL_WIDTH - pad - len(title) - 2)}"

def fmt_row(values, widths):
    return '  '.join(str(v).ljust(w) for v, w in zip(values, widths))

# ── Connect ───────────────────────────────────────────────────────────────────
if not os.path.exists(DB_PATH):
    print(f"ERROR: File not found — {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

lines = []

# ── Header ────────────────────────────────────────────────────────────────────
lines += [
    divider('═'),
    f"  DATABASE DOCUMENTATION",
    f"  File    : {os.path.abspath(DB_PATH)}",
    f"  Size    : {os.path.getsize(DB_PATH) / 1_048_576:.2f} MB",
    f"  Created : {datetime.now().strftime('%Y-%m-%d %H:%M')}",
    divider('═'),
]

# ── Database Summary ──────────────────────────────────────────────────────────
lines.append(section("DATABASE SUMMARY"))
lines.append("")

tables = [r[0] for r in conn.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
)]
indexes  = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'").fetchone()[0]
total_rows = sum(
    conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0] for t in tables
)
total_cols = sum(
    len(conn.execute(f"PRAGMA table_xinfo('{t}')").fetchall()) for t in tables
)
fk_count = sum(
    len(conn.execute(f"PRAGMA foreign_key_list('{t}')").fetchall()) for t in tables
)
db_size_mb = os.path.getsize(DB_PATH) / 1_048_576

lines += [
    f"  {'Tables':<25} {len(tables)}",
    f"  {'Total Columns':<25} {total_cols}",
    f"  {'Total Rows':<25} {total_rows:,}",
    f"  {'Indexes':<25} {indexes}",
    f"  {'Foreign Keys':<25} {fk_count}",
    f"  {'File Size':<25} {db_size_mb:.2f} MB",
    f"  {'SQLite Version':<25} {conn.execute('SELECT sqlite_version()').fetchone()[0]}",
    "",
]

# Largest tables
lines.append(f"  Largest Tables (by row count):")
table_sizes = sorted(
    [(t, conn.execute(f"SELECT COUNT(*) FROM \"{t}\"").fetchone()[0]) for t in tables],
    key=lambda x: x[1], reverse=True
)
for t, count in table_sizes:
    bar = '█' * min(40, int(count / max(r for _, r in table_sizes) * 40))
    lines.append(f"    {t:<25} {count:>8,}  {bar}")
lines.append("")

# ── Table Summary ─────────────────────────────────────────────────────────────
lines.append(section("TABLE SUMMARY"))
lines.append("")

summary_header = fmt_row(['Table', 'Columns', 'Row Count', 'Primary Key(s)'], [30, 10, 12, 40])
lines.append('  ' + summary_header)
lines.append('  ' + divider('─', COL_WIDTH - 2))

for table in tables:
    cols      = conn.execute(f"PRAGMA table_xinfo('{table}')").fetchall()
    row_count = conn.execute(f"SELECT COUNT(*) FROM \"{table}\"").fetchone()[0]
    pks       = [c['name'] for c in cols if c['pk'] > 0]
    pk_str    = ', '.join(pks) if pks else '—'
    lines.append('  ' + fmt_row([table, len(cols), f"{row_count:,}", pk_str], [30, 10, 12, 40]))

# ── Per-Table Detail ──────────────────────────────────────────────────────────
for table in tables:
    lines.append(section(f"TABLE: {table}"))

    # Row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM \"{table}\"").fetchone()[0]
    lines.append(f"\n  Rows: {row_count:,}\n")

    # Columns via table_xinfo
    cols = conn.execute(f"PRAGMA table_xinfo('{table}')").fetchall()
    col_header = fmt_row(['#', 'Column', 'Type', 'Nullable', 'Default', 'PK'], [4, 28, 14, 10, 20, 4])
    lines.append('  ' + col_header)
    lines.append('  ' + divider('─', COL_WIDTH - 2))

    for c in cols:
        nullable = 'NOT NULL' if c['notnull'] else 'NULL'
        default  = str(c['dflt_value']) if c['dflt_value'] is not None else '—'
        pk       = str(c['pk']) if c['pk'] else '—'
        lines.append('  ' + fmt_row(
            [c['cid'], c['name'], c['type'], nullable, default, pk],
            [4, 28, 14, 10, 20, 4]
        ))

    # Indexes
    indexes = conn.execute(f"PRAGMA index_list('{table}')").fetchall()
    if indexes:
        lines.append(f"\n  Indexes:")
        for idx in indexes:
            idx_cols = conn.execute(f"PRAGMA index_info('{idx['name']}')").fetchall()
            col_names = ', '.join(ic['name'] for ic in idx_cols)
            unique    = 'UNIQUE' if idx['unique'] else ''
            lines.append(f"    {unique:7}  {idx['name']}  →  ({col_names})")

    # Foreign keys
    fks = conn.execute(f"PRAGMA foreign_key_list('{table}')").fetchall()
    if fks:
        lines.append(f"\n  Foreign Keys:")
        for fk in fks:
            lines.append(f"    {fk['from']}  →  {fk['table']}.{fk['to']}")

    lines.append("")

# ── Integrity Check ───────────────────────────────────────────────────────────
lines.append(section("INTEGRITY CHECK"))
lines.append("")
result = conn.execute("PRAGMA integrity_check").fetchall()
for r in result:
    lines.append(f"  {r[0]}")
lines.append("")

# ── FK Check ─────────────────────────────────────────────────────────────────
lines.append(section("FOREIGN KEY CHECK"))
lines.append("")
fk_issues = conn.execute("PRAGMA foreign_key_check").fetchall()
if fk_issues:
    for issue in fk_issues:
        lines.append(f"  ISSUE: {dict(issue)}")
else:
    lines.append("  No foreign key violations found.")
lines.append("")

# ── Full Schema ───────────────────────────────────────────────────────────────
lines.append(section("FULL SCHEMA (CREATE STATEMENTS)"))
lines.append("")
schema = conn.execute(
    "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type DESC, name"
).fetchall()
for s in schema:
    lines.append(s[0])
    lines.append("")

lines.append(divider('═'))
lines.append(f"  End of documentation — {len(tables)} tables")
lines.append(divider('═'))

# ── Output ────────────────────────────────────────────────────────────────────
output = '\n'.join(lines)
print(output)

with open(OUT_PATH, 'w', encoding='utf-8') as f:
    f.write(output)

print(f"\n✓ Saved to {OUT_PATH}")
conn.close()
