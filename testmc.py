from market_classifier import MarketClassifier

classifier = MarketClassifier(threshold=0.40)

examples = [
    "Will US CPI exceed 4% in March?",
    "Will the Fed cut rates by June?",
    "Will a celebrity win an Oscar?",
    "Will oil prices surge after supply disruption?"
]

for ex in examples:
    result = classifier.classify(ex)
    print(ex)
    print(result)
    print()
