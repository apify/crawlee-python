"""Data models for tax documents."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, HttpUrl, Field


class TaxDocument(BaseModel):
    """Model for a scraped tax document."""

    url: HttpUrl
    title: str
    content: str
    document_type: Optional[str] = None
    tax_year: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)
