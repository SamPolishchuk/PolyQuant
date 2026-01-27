import numpy as np
import requests
from sentence_transformers import SentenceTransformer


class MarketClassifier:
    def __init__(
        self,
        threshold=0.15,
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
            "Interest Rates": (
                "Central bank interest rate hike cut monetary policy federal reserve ECB "
                "FOMC BoE BoJ pivot terminal rate dot plot yield curve hawk dove easing "
                "tightening neutral rate basis points bps Jerome Powell Fed Chair"
            ),
            "FX": (
                "Foreign exchange currency dollar euro pound yen exchange rate "
                "devaluation appreciation depreciation EURUSD USDJPY GBPUSD DXY "
                "carry trade peg intervention emerging market currencies"
            ),
            "Commodities": (
                "Oil gold gas crude raw materials supply shock brent wti silver copper "
                "rare earths lithium uranium energy security OPEC+ strategic reserve "
                "production cut drilling mining"
            ),
            "Macro": (
                "inflation GDP recession growth CPI unemployment PCE stagflation "
                "deflation purchasing power retail sales manufacturing index PMI "
                "fiscal stimulus deficit debt ceiling labor market soft landing"
            ),
            "Geopolitics": (
                "war conflict military strike sanctions peace ceasefire treaty "
                "Taiwan Strait South China Sea Russia Ukraine NATO trade war "
                "tariffs trade barrier blockade missile naval border dispute"
            ),
            "Elections": (
                "election vote president parliament senate referendum midterm "
                "inauguration primary candidate polls democrat republican "
                "political party leadership transition prime minister cabinet"
            ),
            "Financial Stability": (
                "bank failure default crisis liquidity contagion stress test "
                "insolvency bankruptcy bail out systemic risk credit crunch "
                "bond selloff yield spike financial repression"
            ),
            "Technology": (
                "AI Artificial Intelligence Generative model LLM GPU compute gigafactory "
                "semiconductors chips data center infrastructure humanoid robotics "
                "quantum computing tech regulation big tech magnificent seven"
            ),
            "Crypto": (
                "Bitcoin Ethereum BTC ETH stablecoin ETF SEC regulation "
                "blockchain ledger DeFi hardware wallet mining reward halving "
                "digital assets exchange listing"
            )
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
        prompt = """
You are a financial reasoning engine.

Rules:
- Do NOT use historical facts
- Do NOT estimate likelihood
- Only reason conditionally

Question:
""", text, """

Tasks:
1. Is this market relevant? (yes/no)
2. Which markets are impacted? (Oil, FX, Rates, Equities, Volatility)
3. Direction IF YES (e.g. Oil up, Volatility up, Risk assets down)
4. Is YES risk-on or risk-off?

Output Format:
Respond in strict JSON only. Follow this exact schema:
{
  "relevant": boolean,
  "impacted_markets": ["Oil", "FX", "Rates", "Equities", "Volatility"],
  "conditional_impact": "String describing direction (e.g., 'Rates up, Equities down')",
  "risk_sentiment": "risk-on" | "risk-off"
}
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
