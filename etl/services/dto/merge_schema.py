from pydantic import BaseModel
from typing import Union, Dict, Tuple


class MergeSchema(BaseModel):
    table: str
    select_tb_cols: Dict[str, set]
    join_tb_cols_value: Dict[str, Dict[str, str]]
    filter_tb_col: Tuple[str, str]
    filter_values: Union[set, list]
