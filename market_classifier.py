import numpy as np
import requests
from sentence_transformers import SentenceTransformer


class MarketClassifier:
    def __init__(
        self,
        threshold=0.30,
        ollama_model="llama3:8b-instruct-q4_K_M",
        ollama_url="http://localhost:11434/api/generate"
    ):
        # Embedding model
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.threshold = threshold

        # Ollama config
        self.ollama_model = ollama_model
        self.ollama_url = ollama_url

        # Broad recall categories
        self.categories = {
            "Interest Rates": "Central bank interest rate hike cut monetary policy federal reserve ECB",
            "FX": "Foreign exchange currency dollar euro pound yen exchange rate",
            "Commodities": "Oil gold gas commodities raw materials supply shock",
            "Macro": "inflation interest rates GDP recession growth CPI unemployment",
            "Geopolitics": "war conflict military strike sanctions terrorism peace ceasefire",
            "Elections": "election vote president parliament senate referendum",
            "Financial Stability": "bank failure default crisis liquidity",
            "Technology": "AI Artificial Intelligence Generative ",
        }

        self.category_names = list(self.categories.keys())
        self.category_texts = list(self.categories.values())

        # Precompute category embeddings (ONCE)
        self.category_embeddings = self.model.encode(
            self.category_texts,
            normalize_embeddings=True,
            convert_to_numpy=True
        )

    def embedding_filter(self, text):
        text_embedding = self.model.encode(
            [text],
            normalize_embeddings=True,
            convert_to_numpy=True
        )

        similarities = text_embedding @ self.category_embeddings.T
        best_idx = np.argmax(similarities)
        best_score = similarities[0][best_idx]

        if best_score >= self.threshold:
            return True, self.category_names[best_idx], float(best_score)

        return False, None, float(best_score)

    def ollama_reason(self, text):
        prompt = f"""
You are a financial reasoning engine.

Rules:
- Do NOT use historical facts
- Do NOT estimate likelihood
- Only reason conditionally

Question:
"{text}"

Tasks:
1. Is this market relevant? (yes/no)
2. Which markets are impacted? (Oil, FX, Rates, Equities, Volatility)
3. Direction IF YES (e.g. Oil up, Volatility up, Risk assets down)
4. Is YES risk-on or risk-off?

Respond in strict JSON only.
"""

        response = requests.post(
            self.ollama_url,
            json={
                "model": self.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 200
                }
            },
            timeout=60
        )

        return response.json()["response"]

    def classify(self, text):
        relevant, category, score = self.embedding_filter(text)

        if not relevant:
            return {
                "relevant": False,
                "stage": "embedding_filter",
                "score": score
            }

        llm_output = self.ollama_reason(text)

        return {
            "relevant": True,
            "stage": "llm_reasoning",
            "embedding_category": category,
            "embedding_score": score,
            "llm_analysis": llm_output
        }
