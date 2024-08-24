from typing import Dict, Tuple, Union

from pydantic import BaseModel


class EnrichSchema(BaseModel):
    table: str
    join_tb_cols_value: Dict[str, Dict[str, str]]
    filter_tb_col: Tuple[str, str]
    filter_values: Union[set, list]
    limit: int = 100
    offset: int