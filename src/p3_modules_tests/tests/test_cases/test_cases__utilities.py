import datetime
import pytest
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession

# Gitフォルダーの直下がルートであるたため`src.p3_modules_tests`を指定
from src.p3_modules_tests.utilities import utilities


def test__cast_columns__001():
    # spark セッションを取得
    spark = SparkSession.getActiveSession()

    # ベースのデータフレームを定義
    src_data = [
        ("1", "Alice", "2020-01-01"),
        ("2", "Bob", "2020-02-01"),
        ("3", "Charlie", "2020-03-01"),
    ]
    src_schema = """
    user_id STRING,
    name STRING,
    birthdate STRING
    """
    src_df = spark.createDataFrame(src_data, src_schema)

    # 想定結果ののデータフレームを定義
    exp_data = [
        (1, "Alice", datetime.date(2020, 1, 1)),
        (2, "Bob", datetime.date(2020, 2, 1)),
        (3, "Charlie", datetime.date(2020, 3, 1)),
    ]
    exp_schema = """
    user_id INT,
    name STRING,
    birthdate DATE
    """
    exp_df = spark.createDataFrame(exp_data, exp_schema)

    # テスト対象の関数を実行
    col_mapping_01 = {
        "user_id": "int",
        "birthdate": "date",
    }
    act_df = utilities.cast_columns(src_df, col_mapping_01)

    # データフレームの比較
    assertDataFrameEqual(act_df, exp_df)

def test__cast_columns__002():
    # spark セッションを取得
    spark = SparkSession.getActiveSession()

    # ベースのデータフレームを定義
    src_data = [
        ("1", "Alice", "2020-01-01"),
        ("2", "Bob", "2020-02-01"),
        ("3", "Charlie", "2020-03-01"),
    ]
    src_schema = """
    user_id STRING,
    name STRING,
    birthdate STRING
    """
    src_df = spark.createDataFrame(src_data, src_schema)

    # 想定結果ののデータフレームを定義
    exp_data = [
        ("1", "Alice", "2020-01-01"),
        ("2", "Bob", "2020-02-01"),
        ("3", "Charlie", "2020-03-01"),
    ]
    exp_schema = """
    user_id STRING,
    name STRING,
    birthdate STRING
    """
    exp_df = spark.createDataFrame(exp_data, exp_schema)

    # テスト対象の関数を実行
    col_mapping_01 = {}
    act_df = utilities.cast_columns(src_df, col_mapping_01)

    # データフレームの比較
    assertDataFrameEqual(act_df, exp_df)
