"""
DuckDB Client Core Module

DuckDBへの接続とクエリ実行を管理するコアクラス。
"""

import duckdb
import logging
from pathlib import Path
from typing import Optional, Union, List, Dict, Any
import pandas as pd

# ロガー設定
logger = logging.getLogger(__name__)


class DuckDBClient:
    """
    DuckDBクライアントクラス

    DuckDBデータベースへの接続を管理し、クエリ実行や
    データのインポート/エクスポート機能を提供します。

    Examples:
        >>> client = DuckDBClient("mydb.duckdb")
        >>> result = client.execute_query("SELECT * FROM users")
        >>> client.close()
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        DuckDBクライアントを初期化

        Args:
            db_path: データベースファイルのパス。
                    Noneの場合はインメモリDBを使用。
        """
        self.db_path = str(db_path) if db_path else ":memory:"
        self.conn = None
        self._connect()
        logger.info(f"Connected to DuckDB: {self.db_path}")

    def _connect(self) -> None:
        """データベースに接続"""
        try:
            self.conn = duckdb.connect(self.db_path)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        SQLクエリを実行してDataFrameで結果を返す

        Args:
            query: 実行するSQLクエリ
            params: クエリパラメータ（オプション）

        Returns:
            クエリ結果のDataFrame

        Raises:
            Exception: クエリ実行に失敗した場合

        Examples:
            >>> result = client.execute_query("SELECT * FROM users WHERE age > ?", {"age": 18})
        """
        try:
            logger.debug(f"Executing query: {query}")

            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)

            # 結果をDataFrameに変換
            df = result.fetchdf()
            logger.info(f"Query executed successfully: {len(df)} rows returned")
            return df

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise

    def execute_file(self, sql_file: Union[str, Path]) -> pd.DataFrame:
        """
        SQLファイルを読み込んで実行

        Args:
            sql_file: SQLファイルのパス

        Returns:
            クエリ結果のDataFrame

        Examples:
            >>> result = client.execute_file("queries/analysis.sql")
        """
        sql_path = Path(sql_file)

        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        logger.info(f"Executing SQL file: {sql_file}")

        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        return self.execute_query(sql)

    def show_tables(self) -> pd.DataFrame:
        """
        データベース内のテーブル一覧を取得

        Returns:
            テーブル一覧のDataFrame
        """
        return self.execute_query("SHOW TABLES")

    def describe_table(self, table_name: str) -> pd.DataFrame:
        """
        テーブルのスキーマ情報を取得

        Args:
            table_name: テーブル名

        Returns:
            テーブルスキーマのDataFrame
        """
        return self.execute_query(f"DESCRIBE {table_name}")

    def get_table_sample(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """
        テーブルのサンプルデータを取得

        Args:
            table_name: テーブル名
            limit: 取得する行数

        Returns:
            サンプルデータのDataFrame
        """
        return self.execute_query(f"SELECT * FROM {table_name} LIMIT {limit}")

    def export_to_csv(self, query: str, output_file: Union[str, Path]) -> None:
        """
        クエリ結果をCSVファイルにエクスポート

        Args:
            query: 実行するSQLクエリ
            output_file: 出力CSVファイルのパス

        Examples:
            >>> client.export_to_csv("SELECT * FROM sales", "output.csv")
        """
        output_path = Path(output_file)
        logger.info(f"Exporting query result to CSV: {output_path}")

        result = self.execute_query(query)
        result.to_csv(output_path, index=False)

        logger.info(f"Successfully exported {len(result)} rows to {output_path}")

    def export_to_parquet(self, query: str, output_file: Union[str, Path]) -> None:
        """
        クエリ結果をParquetファイルにエクスポート

        Args:
            query: 実行するSQLクエリ
            output_file: 出力Parquetファイルのパス

        Examples:
            >>> client.export_to_parquet("SELECT * FROM sales", "output.parquet")
        """
        output_path = Path(output_file)
        logger.info(f"Exporting query result to Parquet: {output_path}")

        # DuckDBの組み込みCOPY機能を使用
        self.conn.execute(f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET)")

        logger.info(f"Successfully exported to {output_path}")

    def import_csv(self, csv_file: Union[str, Path], table_name: str) -> None:
        """
        CSVファイルをテーブルにインポート

        Args:
            csv_file: CSVファイルのパス
            table_name: インポート先テーブル名

        Examples:
            >>> client.import_csv("data.csv", "users")
        """
        csv_path = Path(csv_file)

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        logger.info(f"Importing CSV to table '{table_name}': {csv_path}")

        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        self.execute_query(query)

        logger.info(f"Successfully imported {csv_path} to table '{table_name}'")

    def import_parquet(self, parquet_file: Union[str, Path], table_name: str) -> None:
        """
        Parquetファイルをテーブルにインポート

        Args:
            parquet_file: Parquetファイルのパス
            table_name: インポート先テーブル名

        Examples:
            >>> client.import_parquet("data.parquet", "sales")
        """
        parquet_path = Path(parquet_file)

        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_file}")

        logger.info(f"Importing Parquet to table '{table_name}': {parquet_path}")

        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        self.execute_query(query)

        logger.info(f"Successfully imported {parquet_path} to table '{table_name}'")

    def close(self) -> None:
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
            self.conn = None

    def __enter__(self):
        """コンテキストマネージャー: with構文のサポート"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャー: 自動クローズ"""
        self.close()

    def __del__(self):
        """デストラクタ: オブジェクト破棄時の自動クローズ"""
        self.close()
