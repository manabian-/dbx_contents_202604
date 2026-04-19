## 概要

「JEDAI in Osaka 2026 春」にて「生成AIによるDatabricksの開発方法論を改めて考えてみた」というテーマで発表した際の資料です。その発表資料である`20260416_Databricks_AI開発論.pdf`を本ディレクトリに含めています。

[JEDAI in Osaka 2026 春](https://jedai.connpass.com/event/383379/)

## 資料

<script async class="docswell-embed" src="https://www.docswell.com/assets/libs/docswell-embed/docswell-embed.min.js" data-src="https://www.docswell.com/slide/5JW43Q/embed" data-aspect="0.563"></script><div class="docswell-link"><a href="https://www.docswell.com/s/manabian/5JW43Q-2026-04-17-162927">生成AIによるDatabricksの開発方法論を改めて考えてみた【JEDAI in Osaka】 by @manabian</a></div>

## 発表内容の補足

### 生成AIはホームズ、私たちはワトソンという関係が理想

生成AIによる開発は、当初Copilot（副操縦士）という位置づけから始まりましたが、現在ではアウトプットの品質が飛躍的に向上し、私たちの多くの課題を解決してくれる名探偵ホームズのような存在へと進化しています。ただし、シャーロック・ホームズだけでは物語が完結しないように、私たちはワトソン役を担おう、という話をしました。なお、「ワトソン」と聞いてIBM社のIBM Watsonを連想される方がいましたが、本発表で言及しているのはそのサービスではありません。

autoresearchについては、下記のリポジトリで情報が公開されています。

- [karpathy/autoresearch: AI agents running research on single-GPU nanochat training automatically](https://github.com/karpathy/autoresearch)

### 生成AIの出力を鵜呑みにして量産すること

生成AIが出力したコードをそのまま利用しすぎると、コントロールが難しいコードが量産され、いわゆる技術的負債となるコードが大量に残ってしまうリスクがあります。共通ロジックの記述量を減らすためにライブラリ化を行い、それらのコードに対してはpytestで回帰テストを書こう、というお話をしました。

- 構造化データのメダリオンアーキテクチャ
  - [誰も教えてくれないメダリオンアーキテクチャのデザインメソッド：JEDA データエンジニア分科会 #1 #Python - Qiita](https://qiita.com/manabian/items/57373e833df5b4f65184)
- 非構造化データのメダリオンアーキテクチャ
  - [非構造化データのメダリオンアーキテクチャで加速するAIアプリケーション開発 - Vポイントマーケティング｜TECH LABの Tech Blog](https://techblog.vpoint.co.jp/entry/2025/10/21/131455)

### 適切な開発方式や技術検証を持たずに進めること

Databricks Auto Loaderには`ignoreMissingFiles`という重要なオプションがあり、利用する機能についてはドキュメントの参照と動作検証を行うことが重要です。

- [Auto Loaderオプション | Databricks on AWS](https://docs.databricks.com/aws/ja/ingestion/cloud-object-storage/auto-loader/options)

私はプロジェクトの要件定義開始前に技術検討を実施することがあります。少なくとも実装開始前には、利用する技術の仕様を把握しておくことをおすすめします。

- [Databricks のマルチテーブルトランザクション機能の基本的な検証結果 #Databricks - Qiita](https://qiita.com/manabian/items/220e1f07fc9296598808)
- [Databricks における Databricks Utilities taskValues サブユーティリティの基本的な動作確認 #Databricks - Qiita](https://qiita.com/manabian/items/8d9b3dba4cbd602da4c6)
- [Snowflake の CHANGES 句（STREAM）と Databricks の CHANGES 句の動作差異に関する調査結果 #Databricks - Qiita](https://qiita.com/manabian/items/21529d21e6aa3be0a0aa)

### 個人レベルでの活用方法にとどめてしまうこと

開発ライフサイクルにおいて、どのようなコンテキストに基づきどのツールを使うかが重要であると考えています。

どこかで上記のテーマを発表したような気がしていたのですが、下記の記事として投稿していました。Claude CodeやCodexを利用する場合は、ローカルでの開発環境の導入を検討する必要があります。

- [GitHub Copilot による生成AI時代のDatabricks開発フローの新常識 #Python - Qiita](https://qiita.com/manabian/items/d6187a08752dec17b3e0)
