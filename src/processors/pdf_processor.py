import os
from datetime import datetime
from typing import List, Dict, Optional
import re

import pdfplumber
import pandas as pd

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
    
    def extract_exhibitor_table(self, pdf_path: str, 
                                company_col_keywords: List[str] = None) -> List[Dict]:
        """
        Phase 3: Extract exhibitor/company tables from trade fair PDFs
        
        Handles:
        - Multi-column layouts
        - Headers in different rows
        - Contact info extraction (email, phone, website)
        
        Args:
            pdf_path: Path to PDF file
            company_col_keywords: Keywords to identify company name column
            
        Returns:
            List of company dicts with contact info
        """
        if company_col_keywords is None:
            company_col_keywords = [
                'company', 'empresa', 'société', 'firma', 'exhibitor',
                'expositores', 'exposant', 'name', 'nombre', 'nom'
            ]
        
        logger.info(f"Extracting exhibitor table from {pdf_path}")
        companies = []
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    # Extract tables with settings optimized for exhibitor lists
                    tables = page.extract_tables(table_settings={
                        "vertical_strategy": "lines_strict",
                        "horizontal_strategy": "lines_strict",
                        "snap_tolerance": 3,
                        "join_tolerance": 3,
                        "edge_min_length": 3,
                    })
                    
                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        
                        # Find header row (company name column)
                        header_row = self._find_header_row(table, company_col_keywords)
                        
                        if header_row is None:
                            logger.debug(f"No header found in table on page {page_num}")
                            continue
                        
                        # Parse table rows
                        for row in table[header_row + 1:]:
                            if not row or not any(cell and len(str(cell).strip()) > 2 for cell in row):
                                continue
                            
                            company_data = self._parse_exhibitor_row(row, table[header_row])
                            
                            if company_data and company_data.get('company'):
                                companies.append(company_data)
        
        except Exception as e:
            logger.error(f"Error extracting exhibitor table from {pdf_path}: {e}")
        
        logger.info(f"Extracted {len(companies)} companies from {pdf_path}")
        return companies
    
    def _find_header_row(self, table: List[List], keywords: List[str]) -> Optional[int]:
        """
        Find row index containing column headers
        
        Args:
            table: Table data (list of rows)
            keywords: Keywords indicating company column
            
        Returns:
            Row index or None
        """
        for i, row in enumerate(table[:5]):  # Check first 5 rows
            if not row:
                continue
            
            row_text = ' '.join([str(cell).lower() for cell in row if cell])
            
            # Check if any keyword is in the row
            if any(keyword in row_text for keyword in keywords):
                return i
        
        return None
    
    def _parse_exhibitor_row(self, row: List, headers: List) -> Dict:
        """
        Parse a single exhibitor row into structured data
        
        Args:
            row: Table row
            headers: Header row for column names
            
        Returns:
            Company dict with extracted fields
        """
        # Create dict from headers and row
        data = {}
        for i, (header, cell) in enumerate(zip(headers, row)):
            if header and cell:
                header_clean = str(header).lower().strip()
                data[header_clean] = str(cell).strip()
        
        # Extract standard fields
        company = self._find_value(data, ['company', 'empresa', 'exhibitor', 'name', 'nombre', 'nom'])
        email = self._find_value(data, ['email', 'e-mail', 'correo', 'mail'])
        phone = self._find_value(data, ['phone', 'tel', 'telephone', 'teléfono', 'telefone'])
        website = self._find_value(data, ['website', 'web', 'site', 'url', 'sitio'])
        address = self._find_value(data, ['address', 'dirección', 'direccion', 'endereço', 'location'])
        country = self._find_value(data, ['country', 'país', 'pais', 'pays', 'nation'])
        
        # Additional extraction from combined text
        combined_text = ' '.join([str(v) for v in data.values() if v])
        
        if not email:
            email = self._extract_email(combined_text)
        
        if not website:
            website = self._extract_website(combined_text)
        
        if not phone:
            phone = self._extract_phone(combined_text)
        
        return {
            'company': company,
            'email': email,
            'website': website,
            'phone': phone,
            'address': address,
            'country': country,
            'source_type': 'pdf_table',
            'raw_row': data
        }
    
    def _find_value(self, data: Dict, keys: List[str]) -> str:
        """Find first matching value from dict using multiple possible keys"""
        for key in keys:
            for data_key, value in data.items():
                if key in data_key:
                    return value
        return ''
    
    def _extract_email(self, text: str) -> str:
        """Extract email from text using regex"""
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(email_pattern, text)
        return match.group(0) if match else ''
    
    def _extract_website(self, text: str) -> str:
        """Extract website URL from text"""
        url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
        match = re.search(url_pattern, text)
        return match.group(0) if match else ''
    
    def _extract_phone(self, text: str) -> str:
        """Extract phone number from text"""
        # International phone pattern
        phone_pattern = r'\+?[\d\s\-\(\)]{10,20}'
        match = re.search(phone_pattern, text)
        return match.group(0).strip() if match else ''
