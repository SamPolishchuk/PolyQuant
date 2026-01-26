from market_classifier import MarketClassifier
import time

start_time = time.perf_counter()

# Your code here (e.g., Llama inference)
# response = ollama.chat(model='llama3:8b', messages=[...])

classifier = MarketClassifier()

question = "US strikes Iran by January 17, 2026?"

result = classifier.classify(question)

print(result)

end_time = time.perf_counter()
print(f"\n \n \n Total runtime: {end_time - start_time:.4f} seconds")

