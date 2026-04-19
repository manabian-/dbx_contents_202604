
## メダリオンアーキテクチャ

データを、Bronze、Silver、Goldの３層の論理レイヤーで管理する手法です。

![メダリオンアーキテクチャ](https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_01__introduction/delta-lake-medallion-architecture-2.jpeg)

出所: [メダリオンアーキテクチャとは何ですか?](https://www.databricks.com/jp/blog/what-is-medallion-architecture)

実装方法の詳細は公開されていないため、実際の設計指針については自分たちで検討する必要があります。
３層の論理レイヤーでは管理を上位のレイヤーとして捉えて、詳細レイヤーを設計することがおすすめです。

- [誰も教えてくれないメダリオンアーキテクチャの デザインメソッド](https://qiita.com/manabian/items/57373e833df5b4f65184)

Gold レイヤーについて詳細に検討中です。

|  No | レイヤー           | ディレクトリ名案          | 役割                           | 概要                                                                                                                                          |
| --: | -------------- | ----------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
|   1 | `sources`      | m010_sources      | 取り込み元（ソース）定義を管理する層           | プロジェクトの最上流として `sources.yml` を配置し、取り込み対象（DB/スキーマ/テーブル、ロード設定、必要に応じてカラム定義）を一元管理する。                                                             |
|   2 | `staging`      | m020_staging      | 生データを整形し、分析しやすい形に統一する層       | 原則 `sources` のみを参照する `stg_<system>__<entity>.sql` を配置する。型変換、列名の標準化、NULL/空文字の整形、コード値の正規化などを実施する。                          |
|   3 | `intermediate` | m030_intermediate | 複数の staging を束ね、共通ロジックを集約する層 | 下流要件に合わせて必要に応じて `int_<subject>__*.sql` を配置する。結合・重複排除・共通計算・粒度調整を行い、主キーの一意性と NOT NULL の担保など「データ品質の前提」をここで作る。                                        |
|   4 | `standardized`   | m040_standardized   | 共通利用される標準エンティティを提供する層           |  複数ドメイン/複数ソースで共通参照される “標準エンティティ” を `std_<entity>.sql` 等で提供。複数ソース統合、参照データ・コード体系の統一、ビジネスキー確定、重複解消など「再利用前提の標準化」を担当。                                                 |
|   5 | `dimensions`   | m040_dimensions   | ディメンション（マスタ）を確定する層           | `dim_<entity>.sql` を配置する。サロゲートキー採番、Unknown/NA 行の付与、履歴管理（必要なら SCD）など、参照整合性と分析軸としての安定性を確保する。                                                  |
|   6 | `atomic_facts` | m050_atomic_facts | 最小粒度（アトミック）のファクトを確定する層       | 最小粒度の事実を持つ `fct_<name>.sql` を配置する。粒度を明確化し、ディメンションキー付与、メトリクス算出、重複排除などを実施して「再集計の起点」を作る。                                             |
|   7 | `marts`        | m060_marts        | 利用者向けデータマートを提供する層            | `atomic_facts`（必要に応じて `dimensions`）を参照し、用途別に最適化した `fct_*.sql` を配置する。集計、ワイド化、指標定義の固定など、利用者がそのまま使える形に仕上げる。 |
|   8 | `exports`      | m070_exports      | 外部連携・配布向けの最終出力を作る層           | `exp_<target>__*.sql` を配置する。出力仕様に合わせた列順・型・命名、マスキング/匿名化、レコード抽出条件などを適用し、ファイル出力や API 連携に直結する形に整える。                                             |
|   9 | `sandbox`      | m080_sandbox      | 実験・検証を素早く回すための作業用層           | `*.sql` など自由形式で配置する。仮説検証、調査、一時対応、作業途中のモデルを置き、本番品質（テスト/互換/命名規約）の適用対象外として切り分ける。                                                |
|   10 | `deprecated`   | m090_deprecated   | 廃止予定モデルを隔離し、移行期間を支える層        | 廃止予定の `<file_name>.sql` を配置する。互換維持のために一定期間残し、代替先（後継モデル）と削除予定日を明記して段階的に撤去する。                                                                 |

出所: [dbt の models のディレクトリ構成検討](https://zenn.dev/manabian/scraps/c8ac8ec7df86ac)


## Databricks におけるデータエンジニアリングの「命令型」と「宣言型」

### 0. この資料の位置づけ

本資料は、Databricks におけるデータエンジニアリングの実装方法を、**命令型** と **宣言型** という観点から整理するための資料です。
目的は「どちらが優れているか」を決めることではなく、**それぞれが何を自動化し、何を開発者に委ねるのか** を理解することにあります。
実務では Notebook、Lakeflow Spark Declarative Pipelines、dbt を単独で使うより、組み合わせて使うことが多いです。

### 1. 結論

Databricks における代表的な実装スタイルを大まかに分類すると、次のようになります。ご提示の比較表でも、Notebook は命令型、SDP と dbt は宣言型として整理されています。Databricks 公式でも、宣言型の代表として Lakeflow Spark Declarative Pipelines が位置づけられています。 

| 実装方法                                       | 分類  | 主な用途                  |
| ------------------------------------------ | --- | --------------------- |
| Databricks Notebook + Python / SQL         | 命令型 | 柔軟な処理、PoC、複雑な制御       |
| Lakeflow Spark Declarative Pipelines (SDP) | 宣言型 | 継続運用するバッチ / ストリーミング処理 |
| dbt                                        | 宣言型 | SQL 中心の変換、分析基盤のモデル管理  |


manabian による比較記事として下記があります。

- [Databricks にてデータエンジニアリングを実施する方法論の整理](https://zenn.dev/manabian/scraps/8c4b0d15ef1d7f)

### 2. 命令型と宣言型

#### 命令型とは何か

命令型とは、**処理の手順を開発者が細かく記述する** スタイルです。

たとえば Notebook + PySpark では、次のようなことを自分で書きます。

* どの順番で処理するか
* どの DataFrame を中間結果として作るか
* どこで join するか
* どこで write するか
* 失敗時にどう再実行するか
* ログ、監視、品質チェックをどう入れるか

つまり、命令型では **「何をしたいか」だけでなく「どう実行するか」まで自分で持ちます**。
その分、細かい最適化や例外処理を作り込みやすいです。 ([Zenn][1])

#### 宣言型とは何か

宣言型とは、**最終的にどういうデータを得たいかを記述し、処理順序や実行計画の多くをフレームワーク側に委ねる** スタイルです。

Databricks の Lakeflow Spark Declarative Pipelines では、開発者は主に次を定義します。

* どんなテーブルやビューを作りたいか
* 入力と出力の関係
* 品質ルール
* CDC や増分更新の意図

すると、フレームワーク側が依存関係を解決し、実行順序や並列性、失敗時の再試行などを自動化します。

#### 一番大事な違いは「誰が責任を持つか」

命令型と宣言型の差を、実務では次のように捉えると分かりやすいです。

| 観点    | 命令型             | 宣言型       |
| ----- | --------------- | --------- |
| 実行順序  | 開発者が管理          | 基盤が管理しやすい |
| 依存関係  | 自前で設計           | 自動化しやすい   |
| 再試行   | 自前で設計           | 基盤が支援     |
| 柔軟性   | 高い              | 制約の中で高い   |
| 保守性   | チーム次第           | 高めやすい     |
| 学習コスト | 実装自由度ゆえに高くなりやすい | 制約理解が必要   |

**命令型は「自由の代わりに責任が増えます」**。
**宣言型は「制約の代わりに運用が楽になります」**。

#### なぜ宣言型が注目されるのか

Databricks 公式は、SDP の利点として次を強調しています。 ([SDP の利点は何ですか?](https://learn.microsoft.com/ja-jp/azure/databricks/ldp/concepts#what-are-the-benefits-of-sdp))

* **Automatic orchestration**
  実行順序や並列性を自動化しやすいです

* **Declarative processing**
  数百行の手続きコードを、少ない宣言で置き換えやすいです

* **Incremental processing**
  新規データや変更分だけを処理しやすいです




### 3. 具体例で理解する

#### 3.1. 同じ要件を命令型で書く場合

要件:

* Bronze の注文データを読む
* completed のみ抽出する
* 顧客マスタと join する
* Silver テーブルに保存する

命令型では「どう処理するか」を順番に書きます。

```python
bronze = spark.read.table("bronze.orders")
customers = spark.read.table("master.customers")

completed = bronze.filter("status = 'completed'")
joined = completed.join(customers, "customer_id", "left")

(
    joined.select("order_id", "customer_id", "amount", "created_at")
    .write
    .mode("overwrite")
    .saveAsTable("silver.orders")
)
```

**特徴**

* 中間ステップが見えやすいです
* ログを挟みやすいです
* 条件分岐を増やしやすいです
* ただし、ジョブ依存や再実行戦略は別設計になりやすいです

## 3.2 同じ要件を宣言型で書く場合

```python
from pyspark import pipelines as dp

@dp.materialized_view
def silver_orders():
    return (
        spark.read.table("bronze.orders")
        .filter("status = 'completed'")
        .join(spark.read.table("master.customers"), "customer_id", "left")
        .select("order_id", "customer_id", "amount", "created_at")
    )
```

**特徴**

* 出力テーブル中心に考えられます
* 依存関係が自然に表現されます
* パイプライン全体を統一的に扱いやすいです
* ただし、利用できる機能やオブジェクトに制約があります

### 4. まとめ

Databricks におけるデータエンジニアリングでは、命令型と宣言型は対立概念というより、**責任分担の違い** と捉えるのがよいです。

* 命令型は柔軟ですが、設計責任が重いです
* 宣言型は制約がありますが、運用を標準化しやすいです
* 実務では両者を組み合わせるのが普通です
* 技術選定では「書きやすさ」より「運用し続けやすさ」を見るべきです
