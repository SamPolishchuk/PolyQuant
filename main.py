import csv
from market_classifier import MarketClassifier

WINDOW = [-30, -20]

if __name__ == "__main__":

    if WINDOW[0] > WINDOW[1]:
        raise ValueError("Invalid WINDOW: start must be less than end.")

    clf = MarketClassifier(threshold=0.15)

    with open(r"C:\Users\2same\Economics BSc\Quant\PolyQuant\data\market_ids_filtered.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)[WINDOW[0]:WINDOW[1]]

        for row in rows:
            text = row["model_text"]
            result = clf.classify(text)

            print(row["model_text"], "\n", result, "\n\n")
