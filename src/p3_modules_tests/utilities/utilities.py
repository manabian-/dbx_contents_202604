from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType


def cast_columns(
    df: DataFrame,
    schema_map: Dict[str, DataType],
) -> DataFrame:
    # 空の変数が渡された場合にはそのままデータフレームを返す
    if not schema_map:
        return df

    cast_exprs = {
        col_name: F.col(col_name).try_cast(target_type)
        for col_name, target_type in schema_map.items()
    }
    return df.withColumns(cast_exprs)