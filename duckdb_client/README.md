# DuckDB CLI Client

シンプルで使いやすいDuckDBコマンドラインクライアントツール。

## 特徴

- 🚀 シンプルで直感的なCLI
- 📊 クエリ結果の整形表示（複数フォーマット対応）
- 📁 CSV/Parquetファイルのインポート/エクスポート
- 🔍 テーブル一覧・構造確認機能
- 📝 SQLファイル実行対応
- 🎯 インメモリDB/ファイルDB両対応

## インストール

```bash
cd duckdb_client
pip install -e .
```

開発環境の場合：

```bash
pip install -e ".[dev]"
```

## 使用方法

### 基本コマンド

#### 1. テーブル一覧表示

```bash
# インメモリDB
duckdb-cli tables

# ファイルDB
duckdb-cli --db mydb.duckdb tables
```

#### 2. SQLクエリ実行

```bash
# 画面表示
duckdb-cli --db mydb.duckdb query "SELECT * FROM users LIMIT 10"

# CSV形式で出力
duckdb-cli --db mydb.duckdb query "SELECT * FROM users" --output-csv output.csv

# Parquet形式で出力
duckdb-cli --db mydb.duckdb query "SELECT * FROM sales WHERE year = 2024" --output-parquet output.parquet
```

#### 3. SQLファイル実行

```bash
duckdb-cli --db mydb.duckdb file queries.sql
```

#### 4. テーブル構造確認

```bash
duckdb-cli --db mydb.duckdb describe users
```

#### 5. サンプルデータ表示

```bash
# デフォルト10行
duckdb-cli --db mydb.duckdb sample users

# 指定行数
duckdb-cli --db mydb.duckdb sample users --limit 20
```

### データインポート

#### CSVインポート

```bash
duckdb-cli --db mydb.duckdb import-csv data.csv users
```

#### Parquetインポート

```bash
duckdb-cli --db mydb.duckdb import-parquet data.parquet sales
```

### データエクスポート

#### CSV出力

```bash
duckdb-cli --db mydb.duckdb export-csv "SELECT * FROM sales WHERE year = 2024" output.csv
```

#### Parquet出力

```bash
duckdb-cli --db mydb.duckdb export-parquet "SELECT * FROM sales WHERE year = 2024" output.parquet
```

### 表示フォーマット

クエリ結果の表示フォーマットを選択できます：

```bash
# PostgreSQL形式（デフォルト）
duckdb-cli --db mydb.duckdb query "SELECT * FROM users" --format psql

# Grid形式
duckdb-cli --db mydb.duckdb query "SELECT * FROM users" --format grid

# Markdown形式
duckdb-cli --db mydb.duckdb query "SELECT * FROM users" --format markdown

# シンプル形式
duckdb-cli --db mydb.duckdb query "SELECT * FROM users" --format simple
```

## コマンドリファレンス

### グローバルオプション

| オプション | 説明 | デフォルト |
|-----------|------|----------|
| `--db PATH` | データベースファイルパス | `:memory:` |
| `-v, --verbose` | 詳細ログ出力を有効化 | - |

### サブコマンド

| コマンド | 説明 |
|---------|------|
| `tables` | テーブル一覧を表示 |
| `query SQL` | SQLクエリを実行 |
| `file PATH` | SQLファイルを実行 |
| `describe TABLE` | テーブル構造を表示 |
| `sample TABLE` | テーブルのサンプルデータを表示 |
| `import-csv FILE TABLE` | CSVをインポート |
| `import-parquet FILE TABLE` | Parquetをインポート |
| `export-csv SQL FILE` | クエリ結果をCSV出力 |
| `export-parquet SQL FILE` | クエリ結果をParquet出力 |

## 使用例

### 1. データ分析ワークフロー

```bash
# 1. CSVデータをインポート
duckdb-cli --db analytics.duckdb import-csv sales_2024.csv sales

# 2. テーブル構造確認
duckdb-cli --db analytics.duckdb describe sales

# 3. サンプルデータ確認
duckdb-cli --db analytics.duckdb sample sales --limit 5

# 4. 集計クエリ実行
duckdb-cli --db analytics.duckdb query "
  SELECT
    product_category,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
  FROM sales
  GROUP BY product_category
  ORDER BY total_sales DESC
"

# 5. 結果をCSV出力
duckdb-cli --db analytics.duckdb export-csv "
  SELECT * FROM sales WHERE amount > 1000
" high_value_sales.csv
```

### 2. SQLファイルを使った複雑なクエリ

`analysis.sql`:
```sql
-- 月別売上集計
WITH monthly_sales AS (
  SELECT
    strftime(order_date, '%Y-%m') as month,
    SUM(amount) as total
  FROM sales
  GROUP BY month
)
SELECT * FROM monthly_sales
ORDER BY month DESC;
```

実行：
```bash
duckdb-cli --db analytics.duckdb file analysis.sql
```

### 3. Parquetファイルの変換

```bash
# CSV → DuckDB → Parquet
duckdb-cli --db temp.duckdb import-csv input.csv data
duckdb-cli --db temp.duckdb export-parquet "SELECT * FROM data" output.parquet
```

## Pythonライブラリとして使用

CLIだけでなく、Pythonライブラリとしても利用可能：

```python
from duckdb_client import DuckDBClient

# コンテキストマネージャーで使用（推奨）
with DuckDBClient("mydb.duckdb") as client:
    # クエリ実行
    result = client.execute_query("SELECT * FROM users")
    print(result)

    # テーブル一覧
    tables = client.show_tables()
    print(tables)

    # CSVエクスポート
    client.export_to_csv("SELECT * FROM sales", "output.csv")

# または通常の使用方法
client = DuckDBClient("mydb.duckdb")
result = client.execute_query("SELECT COUNT(*) FROM users")
client.close()
```

## 開発

### テスト実行

```bash
pytest
```

### カバレッジ確認

```bash
pytest --cov=duckdb_client --cov-report=html
```

## ライセンス

MIT License

## 貢献

Issue・PRを歓迎します！

## 関連リンク

- [DuckDB公式サイト](https://duckdb.org/)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
