import csv
import json
import time
import os

from classifier import InsiderTradabilityClassifier

# CONFIG

INPUT_CSV = "data/market_ids_filtered.csv"
OUTPUT_CSV = "data/market_ids_insider_only.csv"

ID_COL = "id"   # change to "conditionId" if you prefer

SLEEP_SECONDS = 0.5
LOG_EVERY = 25

# HELPERS

def append_to_csv(row_dict, path, fieldnames):
    write_header = not os.path.exists(path)

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row_dict)


def safe_json_dump(text):
    """
    Ensure JSON-like strings are safely written to CSV.
    """
    try:
        parsed = json.loads(text)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        return text


def load_processed_ids(path, id_col):
    """
    Load IDs that have already been processed from OUTPUT_CSV.
    """
    if not os.path.exists(path):
        return set()

    processed = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if id_col in row:
                processed.add(str(row[id_col]).strip())

    return processed


# MAIN

def clean_insider():
    classifier = InsiderTradabilityClassifier()

    processed_ids = load_processed_ids(OUTPUT_CSV, ID_COL)

    print(f"✅ Input file: {INPUT_CSV}")
    print(f"✅ Output file: {OUTPUT_CSV}")
    print(f"✅ Already processed markets: {len(processed_ids)}")
    print("✅ Starting insider-tradability labelling\n")

    total_seen = 0
    total_processed = 0
    start_time = time.time()

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames + ["insider_tradability_json"]

        for row in reader:
            total_seen += 1
            market_id = str(row[ID_COL]).strip()

            # SKIP ALREADY-PROCESSED MARKETS

            if market_id in processed_ids:
                continue

            total_processed += 1
            question = row["question"]

            print(f"\n▶ Market {total_processed}")
            print(f"Question: {question}")

            # LLM CALL

            llm_output = classifier.classify(question)
            llm_json = llm_output["llm_analysis"]

            print("LLaMA response:")
            print(llm_json)

            # WRITE ROW

            row["insider_tradability_json"] = safe_json_dump(llm_json)
            append_to_csv(row, OUTPUT_CSV, fieldnames)

            processed_ids.add(market_id)

            # LOGGING

            if total_processed % LOG_EVERY == 0:
                elapsed = time.time() - start_time
                print(
                    f"\n--- Progress ---\n"
                    f"Seen: {total_seen}\n"
                    f"Processed (this run): {total_processed}\n"
                    f"Elapsed time: {elapsed:.1f}s\n"
                )

            time.sleep(SLEEP_SECONDS)

    print("\n✅ Labelling complete")
    print(f"Total seen: {total_seen}")
    print(f"Newly processed: {total_processed}")
    print(f"Output written to: {OUTPUT_CSV}")