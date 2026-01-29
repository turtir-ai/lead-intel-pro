import os
from datetime import datetime

import pdfplumber

from src.utils.logger import get_logger
from src.utils.storage import save_text_cache
from src.utils.evidence import record_evidence

logger = get_logger(__name__)

class PdfProcessor:
    def __init__(self, data_dir="data/inputs", evidence_path="outputs/evidence/evidence_log.csv"):
        self.data_dir = data_dir
        self.evidence_path = evidence_path

    def extract_from_pdf(self, pdf_path):
        logger.info(f"Extracting data from PDF: {pdf_path}")
        extracted_data = []

        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if row and any(cell for cell in row if cell and len(str(cell)) > 2):
                                extracted_data.append(" | ".join([str(cell) for cell in row if cell]))

                    text = page.extract_text()
                    if text:
                        extracted_data.append(text)

        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {e}")

        return "\n".join(extracted_data)

    def process_all_pdfs(self):
        all_results = {}
        if not os.path.exists(self.data_dir):
            return all_results
        for filename in os.listdir(self.data_dir):
            if not filename.lower().endswith(".pdf"):
                continue
            full_path = os.path.join(self.data_dir, filename)
            content = self.extract_from_pdf(full_path)
            if not content.strip():
                continue
            source_id = f"file://{full_path}"
            content_hash = save_text_cache(source_id, content)
            record_evidence(
                self.evidence_path,
                {
                    "source_type": "pdf",
                    "source_name": filename,
                    "url": source_id,
                    "title": filename,
                    "snippet": content[:400].replace("\n", " ").strip(),
                    "content_hash": content_hash,
                    "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                },
            )
            all_results[filename] = content
        return all_results
