"""
Schema metadata module for storing additional information about schema versions.
"""

from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel

class SchemaMetadata(BaseModel):
    """Metadata for a schema version."""
    created_at: float
    created_by: str
    description: str
    tags: List[str]
    last_modified: Optional[float] = None
    modified_by: Optional[str] = None
    notes: Optional[str] = None 