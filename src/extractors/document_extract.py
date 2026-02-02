
import logging
import requests
import io
from typing import Dict, Any, Optional

try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    from unstructured.partition.auto import partition as unstructured_partition
except Exception:
    unstructured_partition = None

logger = logging.getLogger(__name__)

class DocumentExtractor:
    """
    V5 Document Extractor: Handles PDF/Docx.
    Strategy:
    1. Try Unstructured (if installed)
    2. Try pdfplumber for PDFs (local)
    3. Fallback to Apache Tika (if running via docker)
    """

    def __init__(self, tika_url="http://localhost:9998"):
        self.tika_url = tika_url
        self.use_tika = self._check_tika()

    def _check_tika(self):
        try:
            requests.get(self.tika_url, timeout=1)
            return True
        except:
            return False

    def extract(self, file_content: bytes, file_type: str = "pdf") -> Dict[str, Any]:
        data = {"text": "", "metadata": {}}

        # 1. Unstructured (best quality, if available)
        if unstructured_partition:
            try:
                text = self._extract_unstructured(file_content)
                if text:
                    data["text"] = text
                    return data
            except Exception as e:
                logger.warning(f"Unstructured extraction failed: {e}")

        # 2. Local PDF fallback
        if file_type == "pdf" and pdfplumber:
            try:
                data["text"] = self._extract_pdfplumber(file_content)
                if data["text"]:
                    return data
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")

        # 2. Heavy Path: Tika Service
        if self.use_tika:
            try:
                return self._extract_tika(file_content)
            except Exception as e:
                logger.warning(f"Tika extraction failed: {e}")

        return data

    def _extract_unstructured(self, content: bytes) -> str:
        with io.BytesIO(content) as f:
            elements = unstructured_partition(file=f)
        return "\n".join([str(el) for el in elements if str(el).strip()])

    def _extract_pdfplumber(self, content: bytes) -> str:
        text_parts = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text_parts.append(page.extract_text() or "")
        return "\n".join([t for t in text_parts if t])

    def _extract_tika(self, content: bytes) -> Dict[str, Any]:
        # Tika Tika
        headers = {'Accept': 'application/json'}
        resp = requests.put(f"{self.tika_url}/tika", data=content, headers=headers)
        if resp.status_code == 200:
            res = resp.json()
            return {
                "text": res.get("X-TIKA:content", "").strip(),
                "metadata": {k: v for k, v in res.items() if not k.startswith("X-TIKA:content")}
            }
        return {"text": "", "metadata": {}}
