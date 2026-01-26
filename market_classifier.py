from sentence_transformers import SentenceTransformer
import numpy as np


class MarketClassifier:
    def __init__(self, threshold=0.40):
        self.model = SentenceTransformer('all-mpnet-base-v2')
        self.threshold = threshold

        self.categories = {
            "Inflation": "Consumer price index CPI inflation price growth deflation",
            "Interest Rates": "Central bank interest rate hike cut monetary policy federal reserve ECB",
            "FX": "Foreign exchange currency dollar euro pound yen exchange rate",
            "GDP": "Gross domestic product economic growth recession output",
            "Commodities": "Oil gold gas commodities raw materials supply shock"
        }

        self.category_names = list(self.categories.keys())
        self.category_texts = list(self.categories.values())

        self.category_embeddings = self.model.encode(
            self.category_texts,
            normalize_embeddings=True
        )

    def classify(self, text):
        text_embedding = self.model.encode(
            [text],
            normalize_embeddings=True
        )

        similarities = np.dot(text_embedding, self.category_embeddings.T)[0]

        best_idx = np.argmax(similarities)
        best_score = similarities[best_idx]
        best_category = self.category_names[best_idx]

        if best_score >= self.threshold:
            return {
                "relevant": True,
                "category": best_category,
                "score": float(best_score)
            }
        else:
            return {
                "relevant": False,
                "category": None,
                "score": float(best_score)
            }
