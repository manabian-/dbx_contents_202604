# 概要

2026年4月に実施したトレーニングのコンテンツです。

## サードパーティソフトウェア

本リポジトリには、`src/p2_medalion_de/openhack2024` 配下の一部コードとして、以下の外部プロジェクトに由来する成果物が含まれています。

- リポジトリ: https://github.com/skotani-db/openhack2024.git
- 著作権者: Shotaro Kotani
- ライセンス: MIT License

当該コードに関する元のライセンス表示および許諾文は、`src/p2_medalion_de/openhack2024/LICENSE` に記載されています。

## ディレクトリ構成

### src配下のディレクトリ

| ディレクトリ | 説明 |
|---|---|
| p1_how_develop_by_ai | 生成AIによるDatabricksの開発方法論についての資料ディレクトリ。「JEDAI in Osaka 2026 春」での発表資料「生成AIによるDatabricksの開発方法論を改めて考えてみた」を含む。 |
| p2_medalion | Databricksメダリオンアーキテクチャの実装例とハンズオン資料。openhack2024（GitHubサブモジュール）を含み、構造化データのメダリオンアーキテクチャ設計やデータエンジニアリングの実装例、Deltaライブテーブルの設定例などが含まれています。 |
| **p3_modules_tests** | Pythonモジュールやテストの実装例とハンズオン資料。 |

## 利用手順

1. Databricks Workspace にログインします。
2. 左側のタブにある`ワークスペース`を選択します。
3. 任意のフォルダにて`作成` -> `Git ファイル`を選択します。
4. `GitリポジトリのURL`に本レポジトリーの URL を入力し、`Gitフォルダを作成`を選択します。
5. Gitレポジトリーのフォルダ上にコンテンツが反映されたことを確認します。