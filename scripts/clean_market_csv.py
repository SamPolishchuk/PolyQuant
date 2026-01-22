#!/usr/bin/env python3
"""
Stream-based CSV cleaner for the market_id.csv file.
- Drops rows with any NA/empty cell
- Drops rows where `category` is in a banned list
- Ensures `id` values are unique (keeps first occurrence)
- Writes cleaned CSV and a small report

Usage:
    python scripts\clean_market_csv.py -i data\market_id.csv -o data\market_id.cleaned.csv --report data\clean_report.txt
"""
import argparse
import csv
import sys
from pathlib import Path

BANNED_CATEGORIES = {
    'sports', 'nba playoffs', 'crypto', 'blanks', 'chess', 'poker', 'art', 'nfts', 'olympics'
}

def is_na_value(val: str) -> bool:
    if val is None:
        return True
    v = str(val).strip()
    if v == '':
        return True
    # common NA strings
    if v.lower() in {'na', 'n/a', 'nan', 'none', '\\tnull\\t'}:
        return True
    return False


def clean_csv(input_path: Path, output_path: Path, report_path: Path, id_field: str = 'id', category_field: str = 'category', required_columns=None, drop_any_na=True):
    if required_columns is None:
        required_columns = [id_field]
    counts = {
        'read': 0,
        'dropped_na': 0,
        'dropped_category': 0,
        'dropped_duplicate_id': 0,
        'written': 0,
    }
    seen_ids = set()
    duplicate_examples = []

    # open input and output
    with input_path.open('r', encoding='utf-8', errors='replace', newline='') as fin, \
         output_path.open('w', encoding='utf-8', newline='') as fout:
        reader = csv.DictReader(fin)
        if id_field not in reader.fieldnames:
            print(f"Warning: id field '{id_field}' not in CSV headers: {reader.fieldnames}", file=sys.stderr)
        # preserve original header order
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            counts['read'] += 1
            # drop rows with NA values
            if drop_any_na:
                # original strict behaviour: drop if any column is NA
                has_na = False
                for k, v in row.items():
                    if is_na_value(v):
                        has_na = True
                        break
                if has_na:
                    counts['dropped_na'] += 1
                    continue
            else:
                # only drop if any of the required_columns are NA
                missing_required = False
                for req in required_columns:
                    if is_na_value(row.get(req)):
                        missing_required = True
                        break
                if missing_required:
                    counts['dropped_na'] += 1
                    continue

            # category filter (case-insensitive, strip)
            category_val = row.get(category_field, '')
            if category_val is not None and str(category_val).strip().lower() in BANNED_CATEGORIES:
                counts['dropped_category'] += 1
                continue

            # enforce unique id
            id_val = row.get(id_field)
            if id_val is None:
                # no id column; treat as NA
                counts['dropped_na'] += 1
                continue

            id_key = str(id_val).strip()
            if id_key in seen_ids:
                counts['dropped_duplicate_id'] += 1
                if len(duplicate_examples) < 10:
                    duplicate_examples.append(id_key)
                continue
            seen_ids.add(id_key)

            # passed all checks -> write
            writer.writerow(row)
            counts['written'] += 1

    # write report
    with report_path.open('w', encoding='utf-8') as r:
        r.write('CSV Clean Report\n')
        r.write('================\n')
        for k in ('read', 'dropped_na', 'dropped_category', 'dropped_duplicate_id', 'written'):
            r.write(f"{k}: {counts[k]}\n")
        r.write('\n')
        if counts['dropped_duplicate_id'] > 0:
            r.write('Duplicate ID examples (kept first occurrence):\n')
            for d in duplicate_examples:
                r.write(d + '\n')

    # print summary for user
    print('CSV clean complete')
    for k in ('read', 'dropped_na', 'dropped_category', 'dropped_duplicate_id', 'written'):
        print(f"{k}: {counts[k]}")
    if counts['dropped_duplicate_id'] > 0:
        print(f"Note: {counts['dropped_duplicate_id']} duplicate rows were dropped. See {report_path} for examples.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stream-clean a large CSV file')
    parser.add_argument('-i', '--input', required=True, help='Input CSV path')
    parser.add_argument('-o', '--output', required=True, help='Output cleaned CSV path')
    parser.add_argument('--report', required=False, default='clean_report.txt', help='Report path')
    parser.add_argument('--id-field', default='id', help='Column name for content ID')
    parser.add_argument('--category-field', default='category', help='Column name for category')
    parser.add_argument('--required-columns', default=None, help='Comma-separated list of columns that must be non-empty (default: id). If provided, only these columns are checked for NA instead of all columns')
    parser.add_argument('--drop-any-na', action='store_true', help='If set, drop rows that have NA in any column (original strict behaviour). By default only required-columns are enforced')
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    report_path = Path(args.report)

    if not input_path.exists():
        print(f"Input file {input_path} not found", file=sys.stderr)
        sys.exit(2)

    req_cols = None
    if args.required_columns:
        req_cols = [c.strip() for c in args.required_columns.split(',') if c.strip()]

    # By default we do NOT drop rows with any NA across all columns, only enforce required columns.
    clean_csv(input_path, output_path, report_path, id_field=args.id_field, category_field=args.category_field, required_columns=req_cols or [args.id_field], drop_any_na=args.drop_any_na)
