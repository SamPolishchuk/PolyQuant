from classifier import MarketClassifier

classifier = MarketClassifier(threshold=0.40)

examples = [
    "Will US CPI exceed 4% in March?",
    "Will the Fed cut rates by June?",
    "Will a celebrity win an Oscar?",
    "Will oil prices surge after supply disruption?",
    "Will the Titans or the Ravens win their January 10th NFL Wild Card matchup?",
]

for ex in examples:
    result = classifier.classify(ex)
    print(ex)
    print(result)
    print()
