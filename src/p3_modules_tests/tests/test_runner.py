# Databricks notebook source
# Databricks Runtime 17.3 LTS （Serverless の environment version 5）より前のクラスターの場合にはインストールする必要あり
# %pip install pytest -q

# COMMAND ----------

# pytest の結果がキャッシュされてしまうため Python を再起動
dbutils.library.restartPython()

# COMMAND ----------

# カレントディレクトリを Repo のルートに設定
import os
 
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
repo_dir = f'/Workspace{repo_root}'
os.chdir(repo_dir)
print(os.getcwd())

# COMMAND ----------

# __pycache__ を作成しないように設定
import sys
sys.dont_write_bytecode = True

# COMMAND ----------

# テストケースを保持しているディレクトリを指定
test_path = 'tests/test_cases'

# COMMAND ----------

# テストを実行
import pytest
pytest_pre_args = []
pytest_discovery_args = [
    "-v",
    "-rsx",
    "--showlocals",
    "--tb=short",
    "-s",
    "--pyargs",
    test_path,
]
pytest_pre_args.extend(pytest_discovery_args)
ut_result = pytest.main(pytest_pre_args)
 
assert ut_result == 0, "The test is failed."


# COMMAND ----------

# end