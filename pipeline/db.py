"""数据库操作模块 - PostgreSQL 连接与操作"""
from __future__ import annotations

from psycopg import connect, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline.config import get_config
from pipeline.constants import POSTGRES_SCHEMA


def get_postgres_dsn(optional=False):
    """获取 PostgreSQL 连接串"""
    dsn = str(get_config("POSTGRES_DSN", "") or "").strip()
    if not dsn and not optional:
        raise RuntimeError("POSTGRES_DSN 未初始化，请先配置 PostgreSQL 连接串。")
    return dsn


def get_public_table_identifier(table_name):
    """生成 PostgreSQL 标识符（public.表名）"""
    normalized_name = str(table_name or "").strip()
    if not normalized_name:
        raise RuntimeError("数据库表名不能为空。")
    return sql.Identifier(POSTGRES_SCHEMA, normalized_name)


def execute_postgres_fetchone(statement, params=None, optional=False):
    """执行查询并返回单行结果"""
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return None

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            row = cur.fetchone()
            return dict(row) if row else None


def execute_postgres_fetchall(statement, params=None, optional=False):
    """执行查询并返回所有行"""
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return []

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            rows = cur.fetchall() or []
            return [dict(row) for row in rows]


def execute_postgres(statement, params=None, optional=False):
    """执行写操作（INSERT/UPDATE/DELETE）"""
    dsn = get_postgres_dsn(optional=optional)
    if not dsn:
        return 0

    with connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or ())
            return cur.rowcount


def execute_postgres_fetchval(statement, params=None, optional=False):
    """执行查询并返回首列值"""
    row = execute_postgres_fetchone(statement, params=params, optional=optional)
    if not row:
        return None
    return next(iter(row.values()))