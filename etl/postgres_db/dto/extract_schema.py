from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class ExtractSchema(BaseModel):
    table: str
    modified: str
    limit: int


class ExtractResponseSchema(BaseModel):
    id: UUID
    modified: datetime
