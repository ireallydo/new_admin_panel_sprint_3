from typing import Optional, Set

from pydantic import BaseModel

from settings.constants import EXTRACTOR_LOAD_LIMIT


class ExtractConfigDTO(BaseModel):
    table_name: str
    modified: Optional[str] = None
    limit: Optional[int] = EXTRACTOR_LOAD_LIMIT


class EnrichConfigDTO(BaseModel):
    base_entity_ids: Optional[Set[str]] = None
    m2m_tb_name: str
    m2m_tb_join_on_col: str
    m2m_tb_filter_col: str
    offset: Optional[int] = None
