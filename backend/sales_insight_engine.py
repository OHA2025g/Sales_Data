import os
import json
from typing import Any, Dict, List, Optional

from huggingface_hub import InferenceClient


class SalesInsightEngine:
    """
    Lightweight Hugging Face Inference API wrapper.
    Reads credentials from env:
      - HF_TOKEN (required)
      - HF_MODEL (optional, default below)
    """

    def __init__(self, token: Optional[str] = None, model: Optional[str] = None):
        self.token = (token or os.environ.get("HF_TOKEN") or "").strip()
        if not self.token:
            raise ValueError("HF_TOKEN is required (set env var HF_TOKEN)")
        self.model = (model or os.environ.get("HF_MODEL") or "mistralai/Mistral-7B-Instruct-v0.3").strip()
        self.client = InferenceClient(token=self.token)

    def _build_prompt(self, dashboard: str, data_summary: Dict[str, Any]) -> str:
        safe = json.dumps(data_summary, ensure_ascii=False)
        return (
            "You are a Senior Sales Analytics Insight Engine.\n"
            "Use ONLY the provided real data summary.\n"
            "Return STRICT JSON with keys: insights, recommendations, action_items.\n"
            "Each value must be an array of strings.\n"
            "Keep it concise and numeric where possible.\n\n"
            f"Dashboard: {dashboard}\n"
            f"Data summary JSON: {safe}\n"
        )

    def generate(self, dashboard: str, data_summary: Dict[str, Any]) -> Dict[str, List[str]]:
        prompt = self._build_prompt(dashboard, data_summary)
        # Use HF text generation endpoint (model must support it)
        text = self.client.text_generation(
            prompt,
            model=self.model,
            max_new_tokens=400,
            temperature=0.2,
            top_p=0.95,
            do_sample=True,
            return_full_text=False,
        )
        # Best-effort JSON extraction
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
        parsed = json.loads(text)
        return {
            "insights": list(parsed.get("insights") or []),
            "recommendations": list(parsed.get("recommendations") or []),
            "action_items": list(parsed.get("action_items") or []),
        }

