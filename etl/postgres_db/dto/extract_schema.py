from pydantic import BaseModel


class ExtractSchema(BaseModel):
    table: str
    modified: str
    limit: int
