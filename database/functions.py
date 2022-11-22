import asyncio
import os
from typing import Any, Mapping, Sequence, TypeAlias

import asyncpg


pool: asyncpg.Pool = ...
Row: TypeAlias = Mapping[str, Any]
url: str = os.getenv('DATABASE_URL')

async def init_pool():
    global pool
    pool = await asyncpg.create_pool(url)


async def execute(query: str, *args):
    async with pool.acquire() as conn:
        await conn.execute(query, *args)

async def executemany(query: str, *args):
    async with pool.acquire() as conn:
        await conn.executemany(query, *args)

async def fetch(query: str, *args) -> Sequence[Mapping]:
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def fetchone(query: str, *args) -> Mapping:
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


if __name__ == '__main__':
    asyncio.run(init())
