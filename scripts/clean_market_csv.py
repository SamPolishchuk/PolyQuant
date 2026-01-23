"""
Stream-based CSV cleaner for the market_id.csv file.
- Drops rows where `closedTime` is NA/empty
- Drops rows where `category` is in a banned list
- Ensures `id` values are unique (keeps first occurrence)
- Drops malformed rows (where JSON commas cause column shifting)
- Writes cleaned CSV and a small report

python scripts\clean_market_csv.py -i data\market_id.csv -o data\market_ids_cleaned.csv --report data\clean_report.txt
"""

from argparse import ArgumentParser
import csv
import sys
from pathlib import Path

BANNED_CATEGORIES = {
    'sports', 'nba playoffs', 'crypto', 'blanks', 'chess',
    'poker', 'art', 'nfts', 'olympics'
}

REQUIRED_NON_NA_COLUMNS = {'id', 'closedTime'}


def is_na_value(val: str) -> bool:
    if val is None:
        return True
    v = str(val).strip()
    if v == '':
        return True
    if v.lower() in {'na', 'n/a', 'nan', 'none', '\\tnull\\t'}:
        return True
    return False


def clean_csv(
    input_path: Path,
    output_path: Path,
    report_path: Path,
    id_field: str = 'id',
    category_field: str = 'category',
    closed_time_field: str = 'closedTime'
):
    counts = {
        'read': 0,
        'dropped_na_closedTime': 0,
        'dropped_category': 0,
        'dropped_duplicate_id': 0,
        'dropped_malformed': 0, # Tracks rows broken by JSON/commas
        'written': 0,
    }

    seen_ids = set()
    duplicate_examples = []

    with input_path.open('r', encoding='utf-8', errors='replace', newline='') as fin, \
         output_path.open('w', encoding='utf-8', newline='') as fout:

        # Use skipinitialspace to handle "Field1, Field2" scenarios
        reader = csv.DictReader(fin, skipinitialspace=True)

        # Clean up fieldnames (removes trailing/leading spaces from headers)
        if reader.fieldnames:
            reader.fieldnames = [f.strip() for f in reader.fieldnames]
        
        fieldnames = reader.fieldnames

        if closed_time_field not in fieldnames:
            print(
                f"Error: required field '{closed_time_field}' not found in CSV headers.\n"
                f"Available fields: {fieldnames[:5]}...",
                file=sys.stderr
            )
            sys.exit(2)

        # QUOTE_MINIMAL ensures that any field containing a comma is wrapped in double quotes
        writer = csv.DictWriter(
            fout,
            fieldnames=fieldnames,
            extrasaction='ignore',
            quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()

        for row in reader:
            counts['read'] += 1

            # --- DATA INTEGRITY CHECK ---
            # If a row has more values than headers, DictReader puts extras in a list under the key None.
            # This is usually caused by unquoted JSON blocks containing commas.
            if row.get(None):
                counts['dropped_malformed'] += 1
                continue

            # Drop rows with NA closedTime
            if is_na_value(row.get(closed_time_field)):
                counts['dropped_na_closedTime'] += 1
                continue

            # Category filter (case-insensitive)
            category_val = row.get(category_field, '')
            if category_val and str(category_val).strip().lower() in BANNED_CATEGORIES:
                counts['dropped_category'] += 1
                continue

            # Enforce unique id
            id_val = row.get(id_field)
            if is_na_value(id_val):
                # Using the specific counter for clarity
                counts['dropped_na_closedTime'] += 1 
                continue

            id_key = str(id_val).strip()
            if id_key in seen_ids:
                counts['dropped_duplicate_id'] += 1
                if len(duplicate_examples) < 10:
                    duplicate_examples.append(id_key)
                continue

            seen_ids.add(id_key)
            writer.writerow(row)
            counts['written'] += 1

    # Write report
    with report_path.open('w', encoding='utf-8') as r:
        r.write('CSV Clean Report\n')
        r.write('================\n')
        for k, v in counts.items():
            r.write(f"{k}: {v}\n")
        r.write('\n')
        if duplicate_examples:
            r.write('Duplicate ID examples (kept first occurrence):\n')
            for d in duplicate_examples:
                r.write(d + '\n')

    # Print summary
    print('\n✅ CSV clean complete')
    for k, v in counts.items():
        print(f"{k:25}: {v}")
    
    if counts['dropped_malformed'] > 0:
        print(f"⚠️  Note: {counts['dropped_malformed']} rows were dropped due to malformed JSON/shifting.")

if __name__ == '__main__':
    parser = ArgumentParser(description='Stream-clean a large CSV file')
    parser.add_argument('-i', '--input', required=True, help='Input CSV path')
    parser.add_argument(
        '-o', '--output',
        default='data/market_ids_cleaned.csv',
        help='Output cleaned CSV path'
    )
    parser.add_argument(
        '--report',
        default='data/clean_report.txt',
        help='Report path'
    )
    parser.add_argument('--id-field', default='id', help='Column name for content ID')
    parser.add_argument('--category-field', default='category', help='Column name for category')
    parser.add_argument('--closed-time-field', default='closedTime', help='Column name for closed time')

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    report_path = Path(args.report)

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input file {input_path} not found", file=sys.stderr)
        sys.exit(2)

    clean_csv(
        input_path,
        output_path,
        report_path,
        id_field=args.id_field,
        category_field=args.category_field,
        closed_time_field=args.closed_time_field
    )

