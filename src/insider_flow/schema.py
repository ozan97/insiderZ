from pydantic import BaseModel, Field, field_validator
from typing import Optional
from datetime import date, datetime

class InsiderTrade(BaseModel):
    # Metadata
    cik: str
    accession_number: str
    filing_date: date
    
    # Issuer Info
    ticker: str
    company_name: str
    
    # Owner Info
    owner_name: str
    owner_title: Optional[str] = None
    is_director: bool = False
    is_officer: bool = False
    is_ten_percent_owner: bool = False
    
    # Transaction Info
    transaction_date: date
    transaction_code: str # P = Purchase, S = Sale, A = Grant, etc.
    shares: float
    price_per_share: float
    total_value: float
    
    # Buy or Sell Logic
    # 'A' (Acquire) usually means Buy/Grant. 'D' (Dispose) means Sell.
    # Note: This is NOT the transaction code (P/S), but the acquisition code in XML.
    acquired_disposed_code: str 

    @field_validator('ticker')
    @classmethod
    def clean_ticker(cls, v):
        return v.upper().strip()