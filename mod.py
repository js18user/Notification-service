# -*- coding: utf-8 -*-
""" script by js18user  """

from asyncio import sleep as sl
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from datetime import timezone as tzs
from enum import Enum
from functools import wraps
from locale import setlocale, LC_ALL
from time import time as t
from typing import Optional
from typing import Union
from aio_pika import DeliveryMode
from aio_pika import Message as Msg
from aio_pika import connect as cnt
from aio_pika import exceptions
# from asyncpg import exceptions as asyncpg_exception
from dateutil.parser import parse
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from fastapi.responses import ORJSONResponse
from loguru import logger as logging
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from pydantic import Field
from pydantic.dataclasses import dataclass
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.cors import CORSMiddleware
from uvicorn import run
from asyncpg_pool import configure_asyncpg
from urls import host
from urls import name
from urls import password
from urls import port
from urls import query_many
from urls import query_ratio
from urls import url_azure
from urls import url_rabbit_google as url_rabbitmq
from urls import user
json = __import__('orjson')
# from contextlib import asynccontextmanager

@dataclass
class Ind:
    interval: timedelta = datetime.now() - datetime.now(tzs.utc).replace(tzinfo=None)
    ine = timedelta(hours=1, )


def memory_dict(f):
    """ Memoization decorator for a function that takes one argument """

    class ms(dict):
        def __missing__(self, key):
            ret = self[key] = f(key)
            return ret

    return ms().__getitem__


class D(dict):
    def __getattr__(self, n):
        try:
            return self[n]
        except KeyError:
            raise AttributeError(n)

    def __setattr__(self, n, val):
        self[n] = val


class UnicornException(Exception):
    def __init__(self, uny: str):
        self.uny = uny


class BaseExc(Exception):
    pass


class Status(str, Enum):
    formed = 'formed'
    sent = 'sent'
    queue = 'queue'
    failure = 'failure'
    expired = 'expired'


class Crud(str, Enum):
    insert = 'insert'
    update = 'update'
    delete = 'delete'
    select = 'select'


class Table(str, Enum):
    client = 'client'
    message = 'message'
    distribution = 'distribution'
    restart = 'restart'


class Client(BaseModel, ):
    id: int = Field(default=None, ge=0, )
    phone: int = Field(default=None, ge=70000000000, le=79999999999, )
    mob: int = Field(default=None, ge=900, le=999, )
    teg: str = Field(default=None, min_length=1, )
    timezone: int = Field(default=None, ge=-11, le=11, )


class ClientUpdate(BaseModel, ):
    phone: int = Field(default=None, ge=70000000000, le=79999999999, )
    mob: int = Field(default=None, ge=900, le=999, )
    teg: str = Field(default=None, min_length=1, )
    timezone: int = Field(default=None, ge=-11, le=11)


class ClientInsert(BaseModel):
    phone: int = Field(ge=70000000000, le=79999999999, )
    mob: int = Field(ge=900, le=999)
    teg: str = Field(min_length=1)
    timezone: int = Field(ge=-11, le=11)


class Message(BaseModel):
    id: int = Field(default=None, ge=0, )
    start_date: datetime = None
    status: Union[Status, None] = None
    id_distribution: Union[int, None] = None
    id_client: Union[int, None] = None


class MessageUpdate(BaseModel):
    start_date: datetime = None
    status: Union[Status, None] = None
    id_distribution: Union[int, None] = None
    id_client: Union[int, None] = None


class MessageInsert(BaseModel):
    start_date: datetime = None
    status: Status
    id_distribution: int
    id_client: int


class Distribution(BaseModel):
    id: int = Field(default=None, ge=0, )
    start_date: datetime = None
    text: str = Field(default=None, min_length=0)
    mob: int = Field(default=None, ge=900, le=999, )
    teg: Union[str, None] = None
    end_date: datetime = None


class DistributionUpdate(BaseModel):
    start_date: datetime = None
    text: Union[str, None] = None
    mob: int = Field(default=None, ge=900, le=999, )
    teg: Union[str, None] = None
    end_date: datetime = None


class DistributionInsert(BaseModel):
    start_date: datetime = None
    text: str = Field(min_length=1, )
    mob: int = Field(ge=900, le=999, )
    teg: str = Field(min_length=1, )
    end_date: datetime = None


def timing_decorator(func_async):
    @wraps(func_async)
    async def wrapper(*args, **kwargs):
        start_time, result = t(), await func_async(*args, **kwargs)
        print(f"Function {func_async.__name__} took {int((t() - start_time) * 1000)} m.sec")
        return result

    return wrapper


def cache_result(func):
    wrapper.cache = {}
    @wraps(func)
    async def wrapper(*args):
        if args in wrapper.cache:
            return wrapper.cache[args]
        result = await func(*args)
        wrapper.cache[args] = result
        return result

    return wrapper

try:
    def db_connect():
        return configure_asyncpg(app, 'postgresql://{user}:{password}@{host}:{port}/{name}'.format(
            user=user,
            name=name,
            password=password,
            port=port,
            host=host, ),
                                 )


    async def send_pika(channel, mess):
        await channel.default_exchange.publish(
            Msg(mess.__str__().encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                ),
            routing_key='queue',
        )


    async def realtime(dt, zone: int, ):
        return dt - ind.ine * zone + ind.interval - ind.ine


    def realtimes(dt, zone: int, ):
        return dt - ind.ine * zone + ind.interval - ind.ine


    async def parsedate(model: dict, ) -> dict:
        if model.get('start_date'):
            model['start_date']: datetime = parse(model['start_date'], ignoretz=True)
        if model.get('end_date'):
            model['end_date']: datetime = parse(model['end_date'], ignoretz=True)
        return model


    def query_update(table, model, adu, ):
        counter, params, cond, vals, qs = 1, [], [], [], "UPDATE {table} SET {columns} WHERE {cond} RETURNING * ;"
        for column, value in model.items():
            match value is not None and value != 0:
                case True:
                    cond.append(f"{column}=${counter}")
                    params.append(value)
                    counter += 1
                case _:
                    pass
        for column, value in adu.items():
            match value is not None and value != 0:
                case True:
                    vals.append(f"{column}=${counter}")
                    params.append(value)
                    counter += 1
                case _:
                    pass
        sql = qs.format(
            table=table, columns=" ,".join(vals), cond=" AND ".join(cond)
        )
        return sql, params


    def query_delete(table, model, fields='*', ):
        if model.get('id'):
            return f"DELETE FROM {table} WHERE id={model.get('id')} RETURNING {fields};"
        else:
            return "DELETE FROM {table} WHERE {where} RETURNING {fields};".format(
                table=table,
                fields=fields,
                where=(" and ".join(["%s='%s'" % (item, model[item])
                                     for item in model.keys()
                                     if model[item]
                                     ])),
            )


    def query_select(table, model, fields="*", ):
        if model.get('id'):
            return f"SELECT {fields} FROM {table} WHERE id={model.get('id')};"
        else:
            where = " and ".join(
                ["%s='%s'" % (item, model[item]) for item in model.keys() if (model[item] is not None)])

            match where == "":
                case True:
                    return f"SELECT {fields} FROM {table};"
                case _:
                    return f"SELECT {fields} FROM {table} WHERE ({where});"


    def query_insert(table, model, fields="*", ):
        length_model = (model.values()).__len__()
        return (f"INSERT INTO {table} ({",".join(list(model.keys()))}) "
                f"VALUES ({",".join([f"${p + 1}" for p in range(length_model)])})"
                f" On Conflict Do Nothing Returning {fields};")


    async def insert(db, table, model, ) -> Sequence[dict]:
        async with db.transaction():
            return await db.fetch(query_insert(table, model), *list(model.values()), )


    async def select(db, table, model, args=None, fields="*", ) -> Sequence[dict]:
        async with db.transaction():
            return await db.fetch(query_select(table, model, fields, ), *(args or []), )


    async def delete(db, table, model, args=None, fields='*', ) -> Sequence[dict]:
        async with db.transaction():
            return await db.fetch(query_delete(table, model, fields), *(args or []), )


    async def update(db, table, model, adu, ) -> Sequence[dict]:
        sql, params = query_update(table, model, adu, )
        async with db.transaction():
            return await db.fetch(sql, *params, )

    # @timing_decorator
    async def update_ids(db, tds, status, ):
        async with db.transaction():
            return await db.execute(f"UPDATE message SET status='{status}' WHERE id in {tds};")

    async def send_message(db,
                           index: int,
                           dict_message: dict,
                           rss: dict,
                           ) -> dict:
        if index == 0:
            await update(db, table=Table.message.value,
                         model=Message(id=dict_message['id']).dict(),
                         adu=MessageUpdate(status=Status.queue.value,
                                           start_date=datetime.now()
                                           ).dict(),
                         )
        dict_message['status'] = Status.queue.value
        pause: int = (dict_message['start_date'].timestamp().__sub__(datetime.now().timestamp()))
        if pause < 0: pause = 0
        await sl(pause, )
        dict_message['status'] = Status.sent.value
        rss[1].append(dict_message['id'])
        return rss


    async def create_queue(db,
                           list_distributions: Sequence[dict],
                           ) -> None:
        await create_queue_messages(db, list_distributions, )
        await create_queue_release(db, list_messages=await m_restart(db, ), )
        return


    async def create_queue_release(db,
                                   list_messages: Sequence[dict],
                                   ) -> None:
        rss: dict = {0: len(list_messages), 1: [], 2: [], }
        for lm_index, dict_message in enumerate(list_messages):
            await send_message(db, lm_index, dict(dict_message), rss, )
        await update_ids(db=db, tds=tuple(rss[1]), status=Status.sent.value, )
        print(f"control: {rss[0]}  {len(rss[1])}  {len(rss[2])}")
        contact = await cnt(url_rabbitmq, )
        async with contact.channel() as session:
            await send_pika(session, list_messages)
        await contact.close()
        return


    async def crms(lc: Sequence[dict],
                   distribution: dict,
                   ) -> Sequence[tuple]:

        lms = []
        for _, client in enumerate(lc):
            match (await realtime(distribution['end_date'], client['timezone']) < datetime.now()):
                case True:
                    status = Status.expired.value
                case _:
                    status = Status.formed.value

            mode = (MessageInsert(status=status,
                                  start_date=parse(str(await realtime(distribution['start_date'],
                                                                      client['timezone']))),
                                  id_distribution=distribution['id'],
                                  id_client=client['id'],
                                  ).dict()).values()
            lms.append(tuple(mode))

        return lms

    async def crml(lc: Sequence[dict],
                   distribution: dict,
                   ) -> Sequence[tuple]:

        return [tuple((MessageInsert(id_distribution=distribution['id'],
                                     id_client=client['id'],
                                     status=Status.formed.value,
                                     start_date=parse(str(await realtime(distribution['start_date'],
                                                                         client['timezone'])))                                                                                                                                       ,
                                    ).dict()).values()) for _, client in enumerate(lc)]


    async def create_queue_messages(db,
                                    ld: Sequence[dict],
                                    ) -> None:

        for _, distribution in enumerate(ld):

            async with db.transaction():
                await db.executemany(query_many, await crml(tuple(await select(db,
                                                        table=Table.client.value,
                                                        fields='id,timezone,phone',
                                                        model=Client(mob=distribution['mob'],
                                                                     teg=distribution['teg'], ).dict()
                                                        )), distribution, ))
        return


    async def m_restart(db, ) -> Sequence[dict]:

        async with db.transaction():
            return await db.fetch(
                f"SELECT d.text, "
                f"d.interval, "
                f"c.phone, "
                f"m.start_date, "
                f"m.status, "
                f"m.id_distribution, "
                f"m.id "
                f"FROM client AS c, message AS m, distribution AS d "
                f"WHERE ( m.status IN ('formed', 'queue', 'failure') "
                f"AND d.id = m.id_distribution  "
                f"AND c.id = m.id_client ) "
                f"ORDER BY m.start_date ; "
            )


    async def seek(db, ) -> Sequence[dict]:
        async with db.transaction():
            return await db.fetch(
                f"SELECT d.*, "
                f"COUNT(m.status) AS com, "
                f"COUNT(m.status) FILTER (WHERE  m.status = 'sent') AS sent, "
                f"COUNT(m.status) FILTER (WHERE  m.status = 'queue') AS queue, "
                f"COUNT(m.status) FILTER (WHERE  m.status = 'formed') AS formed, "
                f"COUNT(m.status) FILTER (WHERE  m.status = 'failure') AS failure, "
                f"COUNT(m.status) FILTER (WHERE  m.status = 'expired') AS expired "
                f"FROM distribution AS d JOIN message AS m  "
                f"ON ( m.id_Distribution=d.id ) "
                f"GROUP BY ( d.id   )  "
                f"ORDER BY ( d.id ) DESC; "
            )


    """    Begin    """
    setlocale(LC_ALL, "de")
    ind, skip = Ind(), '\n'

    app = FastAPI(
        debug=False,
        reload=False,
        workers=3,
        access_log=False,
        title=f"API documentation",
        description=f"A set of Api for completing the task is presented",
        swagger_ui_parameters={f"syntaxHighlight.theme": f"obsidian"},
        default_response_class=ORJSONResponse,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_credentials=False,
        allow_headers=[],
        allow_methods=["GET", "PUT", "POST", "DELETE"],
    )

    @app.middleware("http")
    async def time_crud(request: Request, call_next, ):
        start_time, response = t(), await call_next(request)
        print(f"{"\033[91m"}endpoint execution time:{1000 * (t() - start_time): .0f} m.sec  "
              f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}")
        return response


    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        _, _ = request, exc
        # print(f"RequestValidationError  {request.url}")
        return ORJSONResponse(status_code=400,
                              content=jsonable_encoder([]),
                              )


    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc):
        _, _ = request, exc
        # print(f"StarletteHTTPException  {request.url}")
        return ORJSONResponse(status_code=400,
                              content=jsonable_encoder([]),
                              )


    @app.exception_handler(UnicornException)
    async def unicorn_exception_handler(request: Request, exc: UnicornException):
        print(f"UnicornException  {request.url}")
        return ORJSONResponse(
            status_code=418,
            content={"message": f"Attention! Error with Uvicorn: {exc.uny}"},
        )

    conn = configure_asyncpg(app, url_azure, )
    # conn = db_connect()


    @conn.on_init
    async def initial_db(db):
        with open(f'create_tables.sql', f'r') as sql:
            return await db.execute(sql.read(), )

    Instrumentator().instrument(app).expose(app)

    @app.get('/client', status_code=200, description="", )
    async def client_select(
            db=Depends(conn.connection),
            id: Optional[int] = Query(default=None, ge=0, ),
            phone: int | None = Query(default=None, ),
            mob: int | None = Query(default=None, ge=900, le=999, ),
            teg: str | None = Query(default=None, ),
            timezone: int | None = Query(default=None, ),
    ) -> Sequence[dict]:
        return await select(db,
                            table=Table.client.value,
                            model=json.loads(Client(id=id,
                                                    phone=phone,
                                                    mob=mob,
                                                    teg=teg,
                                                    timezone=timezone,
                                                    ).json()),
                            )


    @app.post('/client', status_code=400, description="", )
    async def client_insert(response: Response,
                            client: ClientInsert,
                            db=Depends(conn.connection),
                            ) -> Sequence[dict]:
        if row := await insert(db, table=Table.client.value, model=json.loads(client.json()), ):
            response.status_code = 200
        return row


    @app.delete('/client', status_code=200, description="", )
    async def client_delete(response: Response,
                            client: Client,
                            db=Depends(conn.connection),
                            ) -> Sequence[dict]:
        if row := await delete(db, table=Table.client.value, model=json.loads(client.json()), ):
            pass
        else: response.status_code = 400
        return row


    @app.put('/client', status_code=200, description="", )
    async def client_update(response: Response,
                            client: Client,
                            upd: ClientUpdate,
                            db=Depends(conn.connection),
                            ) -> Sequence[dict]:
        if row := await update(db, table=Table.client.value,
                           model=json.loads(client.json()),
                           adu=json.loads(upd.json()),
                           ): pass
        else: response.status_code = 400
        return row


    @app.get('/distribution', status_code=200, description="", )
    async def distribution_select(
            db=Depends(conn.connection, ),
            id: int | None = Query(default=None, ge=0, ),
            mob: int | None = Query(default=None, ge=900, le=999, ),
            teg: str | None = Query(default=None, ),
            start_date: datetime | None = Query(default=None, ),
            end_date: datetime | None = Query(default=None, ),
            text: str | None = Query(default=None, ),
    ) -> Sequence[dict]:
        return await select(db,
                            table=Table.distribution.value,
                            model=json.loads(Distribution(id=id,
                                                          mob=mob,
                                                          teg=teg,
                                                          start_date=start_date,
                                                          end_date=end_date,
                                                          text=text,
                                                          ).json()),
                            )


    @app.post('/distribution', status_code=400, description="", )
    async def distribution_insert(response: Response,
                                  distribution: DistributionInsert,
                                  tasks: BackgroundTasks,
                                  db=Depends(conn.connection),
                                  ) -> Sequence[dict]:
        model: dict = await parsedate(json.loads(distribution.json()))
        match model['end_date'] > model['start_date'] and model['end_date'] > datetime.now():
            case True:
                if row := await insert(db,
                                       table=Table.distribution.value,
                                       model=model,
                                       ):
                    response.status_code = 200
                    tasks.add_task(create_queue, db, row, )
            case _: pass
        return row


    @app.delete('/distribution', status_code=200, description="", )
    async def delete_distributions(response: Response,
                                   distribution: Distribution,
                                   db=Depends(conn.connection),
                                   ):
        if row := await delete(db,
                           table=Table.distribution.value,
                           model=await parsedate(json.loads(distribution.json())),
                           ): pass
        else: response.status_code = 400
        return row


    @app.put('/distribution', status_code=400, description="", )
    async def update_distributions(response: Response,
                                   distribution: Distribution,
                                   upd: DistributionUpdate,
                                   background_tasks: BackgroundTasks,
                                   db=Depends(conn.connection),
                                   ):
        adu = await parsedate(json.loads(upd.json()), )
        match adu['end_date'] < datetime.now():
            case True:
                return []
            case _:
                pass
        row = await update(db, table=Table.distribution.value,
                           model=await parsedate(json.loads(distribution.json()), ),
                           adu=adu,
                           )
        match len(row) == 0:
            case True:
                return []
            case _:
                response.status_code = 200
                background_tasks.add_task(
                    create_queue,
                    db,
                    row,
                )
                return row


    @app.get('/message', status_code=200, description="", )
    async def select_message(
            db=Depends(conn.connection),
            id: int | None = Query(default=None, ge=0, ),
            id_distribution: int | None = Query(default=None, ge=0, ),
            id_client: int | None = Query(default=None, ge=0, ),
            status: Status | None = Query(default=None, ),
            start_date: datetime | None = Query(default=None, ),
    ) -> Sequence[dict]:
        return await select(db,
                            table=Table.message.value,
                            model=json.loads(Message(id=id,
                                                     id_distribution=id_distribution,
                                                     id_client=id_client,
                                                     start_date=start_date,
                                                     status=status,
                                                     ).json()),
                            )


    """  next script for Web UI(admin)    """


    @app.get("/")
    async def main():
        return FileResponse("data.html")


    @app.get('/admin/speed', status_code=200, description="Speed Api", )
    async def speed_api():
        return []


    @app.get('/admin/ratio', status_code=200, description="", )
    async def select_ratio(
            db=Depends(conn.connection),
    ):
        async with db.transaction():
            return await db.fetch(query_ratio, )


    @app.get('/admin/distribution', status_code=200, description="", )
    async def select_distributions(db=Depends(conn.connection), ):
        return await seek(db, )


    @app.get("/admin/statistic", status_code=200, description="", )
    async def select_distribution_by_id(
            db=Depends(conn.connection),
            id: int = Query(ge=0, ),
    ):
        async with db.transaction():
            return await db.fetch(
                f"SELECT d.*,COUNT(m.status) AS com,"
                f"COUNT(m.status) FILTER (WHERE m.status='sent') AS sent,"
                f"COUNT(m.status) FILTER (WHERE m.status='queue') AS queue,"
                f"COUNT(m.status) FILTER (WHERE m.status='formed') AS formed,"
                f"COUNT(m.status) FILTER (WHERE m.status='failure') AS failure,"
                f"COUNT(m.status) FILTER (WHERE m.status='expired') AS expired "
                f"FROM Distribution AS d JOIN Message as m  "
                f"ON ( d.id={id} AND m.id_Distribution={id}) "
                f"GROUP BY ( d.id  );"
            )


    @app.get('/admin/message', status_code=200, description="", )
    async def select_messages(
            db=Depends(conn.connection),
            id_distribution: int = Query(ge=0, ),
    ):
        async with db.transaction():
            return await db.fetch(
                f"SELECT m.*,"
                f"c.timezone,"
                f"c.phone "
                f"FROM message AS m, client as c "
                f"WHERE (m.id_distribution={id_distribution} AND "
                f"c.id=m.id_client) "
                f"ORDER BY m.start_date,c.timezone,c.phone,m.status;"
            )


    @app.get('/admin/message/status', status_code=200, description="", )
    async def select_messages_status(
            db=Depends(conn.connection),
            id_distribution: int = Query(ge=0, ),
            status: str = Query(),
    ):
        async with db.transaction():
            return await db.fetch(
                f"SELECT m.*, "
                f"c.timezone AS timezone, "
                f"c.phone AS phone "
                f"FROM message AS m, client as c "
                f"WHERE (m.id_distribution={id_distribution} AND "
                f"m.status='{status}' AND "
                f"c.id=m.id_client ) "
                f"ORDER BY m.start_date, c.timezone, c.phone;"
            )
except:
    logging.info(f"Basis error")
finally: pass


if __name__ == "__main__":
    
    try:
        run('mod:app', host='0.0.0.0', port=80, )  # reload=True, )
    except KeyboardInterrupt:
        pass
