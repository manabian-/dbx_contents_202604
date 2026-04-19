# Databricks notebook source
# MAGIC %md
# MAGIC ## 処理の共通化

# COMMAND ----------

# MAGIC %md
# MAGIC ### ベースのコードを確認

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# ベースのデータフレームを定義
data = [
    ("1", "Alice", "2020-01-01"),
    ("2", "Bob", "2020-02-01"),
    ("3", "Charlie", "2020-03-01"),
]
schema = """
user_id STRING,
name STRING,
birthdate STRING
"""
base_df = spark.createDataFrame(data, schema)

# スキーマとデータの表示
base_df.printSchema()
base_df.display()

# COMMAND ----------

# データ型の変更
df2 = base_df.withColumn("user_id", F.col("user_id").cast("int"))
df2 = df2.withColumn("birthdate", F.col("birthdate").cast("date"))

# スキーマとデータの表示
print("-- base_df のスキーマ")
base_df.printSchema()
print("-- 処理後のスキーマ")
df2.printSchema()
df2.display()

# COMMAND ----------

# データ型の変更
base_df.createOrReplaceTempView("_temp_table_01")
df3 = spark.sql("""
SELECT
    try_CAST(user_id AS int) AS user_id,
    name,
    try_CAST(birthdate AS date) AS birthdate
    FROM
        _temp_table_01
""")

# スキーマとデータの表示
print("-- base_df のスキーマ")
base_df.printSchema()
print("-- 処理後のスキーマ")
df3.printSchema()
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe in, Dataframe out による関数化

# COMMAND ----------

# 変数によりデータ型変換できるように変更
col_mapping_01 = {
    "user_id": "int",
    "birthdate": "date",
}

# COMMAND ----------

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DataType


def cast_columns(
    df: DataFrame,
    schema_map: Dict[str, DataType],
) -> DataFrame:
    cast_exprs = {
        col_name: F.col(col_name).try_cast(target_type)
        for col_name, target_type in schema_map.items()
    }
    return df.withColumns(cast_exprs)

# COMMAND ----------

# 関数によりデータ型を変更
df_10 = cast_columns(base_df, col_mapping_01)


# スキーマとデータの表示
print("-- base_df のスキーマ")
base_df.printSchema()
print("-- 処理後のスキーマ")
df_10.printSchema()
df_10.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 空の変数が渡された場合の動作を確認

# COMMAND ----------

# 空を渡した場合の動作を確認
col_mapping_02 = {}

# ToDo ここのアウトを除外してエラーとなることを確認
# df_11 = cast_columns(df, col_mapping_02)

# COMMAND ----------

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

# COMMAND ----------

# 空を渡した場合の動作を確認
col_mapping_02 = {}

# ToDo ここのアウトを除外してエラーとなることを確認
df_11 = cast_columns(base_df, col_mapping_02)

# スキーマとデータの表示
print("-- base_df のスキーマ")
base_df.printSchema()
print("-- 処理後のスキーマ")
df_11.printSchema()
df_11.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## テスト

# COMMAND ----------

# MAGIC %md
# MAGIC ### テストの基本的な手順

# COMMAND ----------

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

# スキーマとデータの表示
src_df.printSchema()
src_df.display()

# COMMAND ----------

# 関数によりデータ型を変更
col_mapping_01 = {
    "user_id": "int",
    "birthdate": "date",
}
act_df = cast_columns(src_df, col_mapping_01)

# スキーマとデータの表示
print("-- src_df のスキーマ")
src_df.printSchema()
print("-- 処理後のスキーマ")
act_df.printSchema()
act_df.display()

# COMMAND ----------

import datetime

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

# スキーマとデータの表示
exp_df.printSchema()
exp_df.display()

# COMMAND ----------

# Tips: 想定結果の作り方
print("-- Row")
print(act_df.collect())
print("")

# act_df.collect() から辞書型変数を作成
print("-- 辞書型変数で表示")
dict_list = [row.asDict() for row in act_df.collect()]
print(dict_list)

# COMMAND ----------

# データフレームの比較
from pyspark.testing import assertDataFrameEqual

assertDataFrameEqual(act_df, exp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### assertDataFrameEqual の動作確認
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - ドキュメント
# MAGIC     - [pyspark.testing.assertDataFrameEqual — PySpark 4.1.1 documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.testing.assertDataFrameEqual.html)
# MAGIC - コミュニティ記事
# MAGIC     - [PySpark の Testing 機能の１つである assertDataFrameEqual に対する基本的な検証結果 #Python - Qiita](https://qiita.com/manabian/items/524f2a5263db18de42da)

# COMMAND ----------

# 誤っている場合の確認: スキーマが異なる場合
# ToDo コメントアウトを除外して実行するとエラーとなる
# assertDataFrameEqual(act_df, src_df)

# COMMAND ----------

# 誤っている場合の確認: データが異なる場合
bad_data_01 = [
    (1, "Alice", datetime.date(2020, 1, 1)),
    (2, "Bob", datetime.date(2020, 2, 1)),
]
bad_schema_01 = """
user_id INT,
name STRING,
birthdate DATE
"""
bad_df_01 = spark.createDataFrame(bad_data_01, bad_schema_01)

# ToDo コメントアウトを除外して実行するとエラーとなる
# assertDataFrameEqual(act_df, bad_df_01)

# COMMAND ----------

# 誤っている場合の確認: が異なる場合
bad_data_02 = [
    ("1", "Alice", datetime.date(2020, 1, 1)),
    ("2", "Bob", datetime.date(2020, 2, 1)),
    ("3", "Charlie", datetime.date(2020, 3, 1)),
]
bad_schema_02 = """
user_id STRING,
name STRING,
birthdate DATE
"""
bad_df_02 = spark.createDataFrame(bad_data_02, bad_schema_02)

# ToDo コメントアウトを除外して実行するとエラーとなる
# assertDataFrameEqual(act_df, bad_df_02)

# COMMAND ----------

# MAGIC %md
# MAGIC ## pytest による回帰テスト化

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <a href="$./tests/test_runner">テスト実行ノートブック: ./test/test_runner</a> に移動して、テストを実行してください。
# MAGIC
# MAGIC 下記のようなディレクトリ構成により pytest によりテストの実施が可能です。
# MAGIC
# MAGIC ```text
# MAGIC |--utilities
# MAGIC |  |--utilities.py
# MAGIC |--tests
# MAGIC |  |--test_cases
# MAGIC |  |  |--test_cases__utilities.py
# MAGIC |  |--test_runner <-- テストを実行するノートブック
# MAGIC ```
# MAGIC
# MAGIC **参考リンク**
# MAGIC
# MAGIC - [Databricks Workspace 上で pytest によるテストケース作成とテスト実行を行う方法 #Python - Qiita](https://qiita.com/manabian/items/c31a6498b2db1b78ac1b)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC End