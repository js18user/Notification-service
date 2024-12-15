from __future__ import annotations
import asyncpg
from typing import Callable


async def noop(db: asyncpg.Connection):
    return


class configure_asyncpg:
    def __init__(
        self,
        app: FastAPI,
        dsn: str,
        *,
        init_db: Callable = None,
        pool=None,
        **options,
    ):
        self.app = app
        self.dsn = dsn
        self.init_db = init_db
        self.con_opts = options
        self._pool = pool
        self.app.router.add_event_handler("startup", self.on_connect)
        self.app.router.add_event_handler("shutdown", self.on_disconnect)

    async def on_connect(self):
        if self._pool:
            self.app.state.pool = self._pool
            return
        pool = await asyncpg.create_pool(dsn=self.dsn, **self.con_opts)
        async with pool.acquire() as db:
            await self.init_db(db)
        self.app.state.pool = pool

    async def on_disconnect(self):
        if self._pool:
            return
        await self.app.state.pool.close()

    def on_init(self, func):
        self.init_db = func
        return func

    @property
    def pool(self):
        return self.app.state.pool

    async def connection(self):
        async with self.pool.acquire() as db:
            yield db

    async def transaction(self):
        async with self.pool.acquire() as db:
            txn = db.transaction()
            await txn.start()
            try:
                yield db
            except:  # noqa
                await txn.rollback()
                raise
            else:
                await txn.commit()

    atomic = transaction
