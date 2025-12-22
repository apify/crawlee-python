"""Data models for tax documents."""

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl


class TaxDocument(BaseModel):
    """Model for a scraped tax document."""

    url: HttpUrl
    title: str
    content: str
    document_type: str | None = None
    tax_year: str | None = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict = Field(default_factory=dict)
