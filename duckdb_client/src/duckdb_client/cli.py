"""
DuckDB Client CLI Module

コマンドラインインターフェースを提供するモジュール。
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional
from tabulate import tabulate

from .client import DuckDBClient

# ロガー設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def print_dataframe(df, format_style: str = "psql"):
    """
    DataFrameを整形して表示

    Args:
        df: 表示するDataFrame
        format_style: テーブル表示スタイル（psql, grid, simple等）
    """
    if len(df) == 0:
        print("No results.")
        return

    print(tabulate(df, headers='keys', tablefmt=format_style, showindex=False))
    print(f"\n({len(df)} rows)")


def cmd_query(args):
    """クエリ実行コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            result = client.execute_query(args.query)

            # CSV出力オプションが指定されている場合
            if args.output_csv:
                result.to_csv(args.output_csv, index=False)
                print(f"Successfully exported {len(result)} rows to: {args.output_csv}")
            # Parquet出力オプションが指定されている場合
            elif args.output_parquet:
                client.export_to_parquet(args.query, args.output_parquet)
                print(f"Successfully exported to: {args.output_parquet}")
            # 通常の画面表示
            else:
                print_dataframe(result, args.format)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            sys.exit(1)


def cmd_file(args):
    """SQLファイル実行コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            result = client.execute_file(args.file)
            print_dataframe(result, args.format)
        except Exception as e:
            logger.error(f"File execution failed: {e}")
            sys.exit(1)


def cmd_tables(args):
    """テーブル一覧表示コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            result = client.show_tables()
            print_dataframe(result, args.format)
        except Exception as e:
            logger.error(f"Failed to show tables: {e}")
            sys.exit(1)


def cmd_describe(args):
    """テーブル構造表示コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            result = client.describe_table(args.table)
            print_dataframe(result, args.format)
        except Exception as e:
            logger.error(f"Failed to describe table: {e}")
            sys.exit(1)


def cmd_sample(args):
    """サンプルデータ表示コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            result = client.get_table_sample(args.table, args.limit)
            print_dataframe(result, args.format)
        except Exception as e:
            logger.error(f"Failed to get sample data: {e}")
            sys.exit(1)


def cmd_export_csv(args):
    """CSV出力コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            client.export_to_csv(args.query, args.output)
            print(f"Successfully exported to: {args.output}")
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            sys.exit(1)


def cmd_export_parquet(args):
    """Parquet出力コマンド"""
    with DuckDBClient(args.db) as client:
        try:
            client.export_to_parquet(args.query, args.output)
            print(f"Successfully exported to: {args.output}")
        except Exception as e:
            logger.error(f"Parquet export failed: {e}")
            sys.exit(1)


def cmd_import_csv(args):
    """CSVインポートコマンド"""
    with DuckDBClient(args.db) as client:
        try:
            client.import_csv(args.file, args.table)
            print(f"Successfully imported {args.file} to table '{args.table}'")
        except Exception as e:
            logger.error(f"CSV import failed: {e}")
            sys.exit(1)


def cmd_import_parquet(args):
    """Parquetインポートコマンド"""
    with DuckDBClient(args.db) as client:
        try:
            client.import_parquet(args.file, args.table)
            print(f"Successfully imported {args.file} to table '{args.table}'")
        except Exception as e:
            logger.error(f"Parquet import failed: {e}")
            sys.exit(1)


def create_parser() -> argparse.ArgumentParser:
    """
    コマンドライン引数パーサーを作成

    Returns:
        設定済みのArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog='duckdb-cli',
        description='Simple CLI tool for DuckDB',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # テーブル一覧表示
  duckdb-cli --db mydb.duckdb tables

  # クエリ実行
  duckdb-cli --db mydb.duckdb query "SELECT * FROM users LIMIT 10"

  # SQLファイル実行
  duckdb-cli --db mydb.duckdb file queries.sql

  # テーブル構造確認
  duckdb-cli --db mydb.duckdb describe users

  # CSV出力
  duckdb-cli --db mydb.duckdb export-csv "SELECT * FROM sales" output.csv

  # CSVインポート
  duckdb-cli --db mydb.duckdb import-csv data.csv users
        """
    )

    # 共通オプション
    parser.add_argument(
        '--db',
        type=str,
        default=':memory:',
        help='Database file path (default: in-memory)'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )

    # サブコマンド
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # query コマンド
    parser_query = subparsers.add_parser('query', help='Execute SQL query')
    parser_query.add_argument('query', type=str, help='SQL query to execute')
    parser_query.add_argument(
        '--format',
        type=str,
        default='psql',
        choices=['psql', 'grid', 'simple', 'plain', 'markdown'],
        help='Output format (default: psql)'
    )
    parser_query.add_argument(
        '--output-csv',
        dest='output_csv',
        type=str,
        metavar='FILE',
        help='Export result to CSV file instead of displaying'
    )
    parser_query.add_argument(
        '--output-parquet',
        dest='output_parquet',
        type=str,
        metavar='FILE',
        help='Export result to Parquet file instead of displaying'
    )
    parser_query.set_defaults(func=cmd_query)

    # file コマンド
    parser_file = subparsers.add_parser('file', help='Execute SQL file')
    parser_file.add_argument('file', type=str, help='SQL file path')
    parser_file.add_argument(
        '--format',
        type=str,
        default='psql',
        choices=['psql', 'grid', 'simple', 'plain', 'markdown'],
        help='Output format (default: psql)'
    )
    parser_file.set_defaults(func=cmd_file)

    # tables コマンド
    parser_tables = subparsers.add_parser('tables', help='List all tables')
    parser_tables.add_argument(
        '--format',
        type=str,
        default='psql',
        choices=['psql', 'grid', 'simple', 'plain', 'markdown'],
        help='Output format (default: psql)'
    )
    parser_tables.set_defaults(func=cmd_tables)

    # describe コマンド
    parser_describe = subparsers.add_parser('describe', help='Describe table structure')
    parser_describe.add_argument('table', type=str, help='Table name')
    parser_describe.add_argument(
        '--format',
        type=str,
        default='psql',
        choices=['psql', 'grid', 'simple', 'plain', 'markdown'],
        help='Output format (default: psql)'
    )
    parser_describe.set_defaults(func=cmd_describe)

    # sample コマンド
    parser_sample = subparsers.add_parser('sample', help='Show sample data from table')
    parser_sample.add_argument('table', type=str, help='Table name')
    parser_sample.add_argument(
        '--limit',
        type=int,
        default=10,
        help='Number of rows to display (default: 10)'
    )
    parser_sample.add_argument(
        '--format',
        type=str,
        default='psql',
        choices=['psql', 'grid', 'simple', 'plain', 'markdown'],
        help='Output format (default: psql)'
    )
    parser_sample.set_defaults(func=cmd_sample)

    # export-csv コマンド
    parser_export_csv = subparsers.add_parser('export-csv', help='Export query result to CSV')
    parser_export_csv.add_argument('query', type=str, help='SQL query')
    parser_export_csv.add_argument('output', type=str, help='Output CSV file path')
    parser_export_csv.set_defaults(func=cmd_export_csv)

    # export-parquet コマンド
    parser_export_parquet = subparsers.add_parser('export-parquet', help='Export query result to Parquet')
    parser_export_parquet.add_argument('query', type=str, help='SQL query')
    parser_export_parquet.add_argument('output', type=str, help='Output Parquet file path')
    parser_export_parquet.set_defaults(func=cmd_export_parquet)

    # import-csv コマンド
    parser_import_csv = subparsers.add_parser('import-csv', help='Import CSV file to table')
    parser_import_csv.add_argument('file', type=str, help='CSV file path')
    parser_import_csv.add_argument('table', type=str, help='Target table name')
    parser_import_csv.set_defaults(func=cmd_import_csv)

    # import-parquet コマンド
    parser_import_parquet = subparsers.add_parser('import-parquet', help='Import Parquet file to table')
    parser_import_parquet.add_argument('file', type=str, help='Parquet file path')
    parser_import_parquet.add_argument('table', type=str, help='Target table name')
    parser_import_parquet.set_defaults(func=cmd_import_parquet)

    return parser


def main():
    """CLIメインエントリーポイント"""
    parser = create_parser()
    args = parser.parse_args()

    # ログレベル設定
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")

    # コマンドが指定されていない場合はヘルプを表示
    if not args.command:
        parser.print_help()
        sys.exit(0)

    # コマンド実行
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
