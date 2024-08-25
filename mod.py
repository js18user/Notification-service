# -*- coding: utf-8 -*-
""" script by js18user  """
import uvicorn
import asyncio
import locale
import time

import orjson as json
from datetime import datetime, timedelta
from datetime import timezone as tzs

from enum import Enum
from functools import wraps
from typing import Union, Optional

from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Depends, BackgroundTasks, Query, Response, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi_asyncpg import configure_asyncpg
from asyncpg import exceptions as asyncpg_exception
from aio_pika import exceptions
from aio_pika import DeliveryMode
from aio_pika import Message as Msg
from aio_pika import connect as cnt
from dateutil.parser import parse
from loguru import logger as logging
from pydantic import BaseModel, Field

from urls import db_host_twp as host
from urls import db_port_twp as port
from urls import db_user_twp as user
from urls import db_name_twp as name
from urls import db_password_twp as password
from urls import query_many

from urls import url_rabbit_google as url_rabbitmq
from dataclasses import dataclass


@dataclass
class Ind:
    name: str = 'for background task'
    interval: timedelta = datetime.now() - datetime.now(tzs.utc).replace(tzinfo=None)
    ine = timedelta(hours=1, )


class MyMiddleware:
    def __init__(self, some_attribute: str, ):
        self.some_attribute = some_attribute

    async def __call__(self, request: Request, call_next, ) -> None:
        start_time, response = time.time(), await call_next(request)
        print(f"{"\033[91m"}endpoint execution time:{1000*(time.time() - start_time): .0f} m.sec  "
              f"{datetime.now().strftime("%d-%m-%Y %H:%M:%S")}")
        return response


class D(dict):
    def __getattr__(self, n):
        try:
            return self[n]
        except KeyError:
            raise AttributeError(n)

    def __setattr__(self, n, val):
        self[n] = val


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


class Client(BaseModel, title="The description of the item", ):
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
        start_time = time.time()
        result = await func_async(*args, **kwargs)
        print(f"Function {func_async.__name__} took {int((time.time() - start_time)*1000)} m.sec")
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


    async def realtime(dt, zone, ):
        return dt - ind.ine*zone + ind.interval - ind.ine


    async def parsedate(model, ):
        match  model.get('start_date'):
            case None:
                pass
            case _:
                model['start_date'] = parse(model['start_date'], ignoretz=True)

        match model.get('end_date'):
            case None:
                pass
            case _:
                model['end_date'] = parse(model['end_date'], ignoretz=True)
        return model


    def query_update(table, model, adu, ):
        counter, params, cond, vals, qs = 1, [], [], [], "UPDATE {table} SET {columns} WHERE {cond} RETURNING * ;"

        for column, value in model.items():
            match value is not None and value != 0:
                case True:
                    cond.append(f"{column}=${counter}")
                    params.append(value)
                    counter += 1
                case _: pass

        for column, value in adu.items():
            match value is not None and value != 0:
                case True:
                    vals.append(f"{column}=${counter}")
                    params.append(value)
                    counter += 1
                case _: pass
        sql = qs.format(
            table=table, columns=" ,".join(vals), cond=" AND ".join(cond)
        )
        return sql, params


    def query_delete(table, model, fields='*', ):
        match model.get('id') is None:
            case True:
                return "DELETE FROM {table}  WHERE  {where}  RETURNING {fields} ;".format(
                    table=table,
                    fields=fields,
                    where=(" and ".join(["%s='%s'" % (item, model[item])
                                        for item in model.keys()
                                        if (model[item] is not None)
                                         ])),
                )
            case _: return f"DELETE FROM {table}  WHERE id = {model.get('id')}  RETURNING {fields} ;"


    def query_select(table, model, fields="*", ):
        match model.get('id') is None:
            case True:
                where = " and ".join(
                    ["%s='%s'" % (item, model[item]) for item in model.keys() if (model[item] is not None)])

                match where == "":
                    case True: return f"SELECT {fields} FROM {table} ;"
                    case _: return f"SELECT {fields} FROM {table} WHERE ({where}) ;"
            case _: return f"SELECT {fields} FROM {table}  WHERE id = {model.get('id')};"


    def query_insert(table, model, fields="*", ):
        length_model = len(model.values())
        return "INSERT INTO {table} ({columns}) VALUES ({values}) On Conflict Do Nothing Returning {fields};".format(
            table=table,
            values=",".join([f"${p + 1}" for p in range(length_model)]),
            columns=",".join(list(model.keys())),
            fields=fields,
        )


    async def insert(db, table, model, ):
        async with db.transaction():
            return await db.fetch(query_insert(table, model), *list(model.values()), )

    async def select(db, table, model, args=None, fields="*", ):
        return await db.fetch(query_select(table, model, fields, ), *(args or []), )


    async def delete(db, table, model, args=None, fields='*', ):
        async with db.transaction():
            return await db.fetch(query_delete(table, model, fields), *(args or []), )


    async def update(db, table, model, adu, ):
        sql, params = query_update(table, model, adu, )
        async with db.transaction():
            return await db.fetch(sql, *params, )

    @timing_decorator
    async def update_ids(db, tds, status, ):
        async with db.transaction():
            return await db.execute(f"UPDATE message SET status = '{status}' WHERE id in {tds} ;")


    async def send_message(db, session, index: int, dict_message: dict, rss: dict, ):

        match index:
            case 0:
                await update(db, table=Table.message.value,
                             model=Message(id=dict_message['id']).dict(),
                             adu=MessageUpdate(status=Status.queue.value,
                                               start_date=datetime.now()
                                               ).dict(),
                             )
            case _:
                dict_message['status'] = Status.queue.value
        pause: int = (dict_message['start_date'].timestamp().__sub__(datetime.now().timestamp()))
        match pause < 0:
            case True: pause = 0
            case _: pass
        await asyncio.sleep(pause, )
        dict_message['status'] = Status.sent.value
        try:
            await send_pika(session, dict_message)
        except exceptions as error:
            rss[2].append(dict_message['id'])
            logging.info(f"There are problem with aio_pika{skip} Error: {error}")
        else:
            rss[1].append(dict_message['id'])
        finally:
            pass
        return rss


    async def create_queue(db, list_distributions, ):
        await create_queue_messages(db, list_distributions, )
        await create_queue_release(db, list_messages=await m_restart(db, ), )
        return


    async def create_queue_release(db, list_messages, ):
        """ Where is None: current operation; Where is True: restart operation """

        rss: dict = {0: len(list_messages), 1: [], 2: [], }

        contact = await cnt(url_rabbitmq, )
        async with contact.channel() as session:
            for lm_index, dict_message in enumerate(list_messages):
                await send_message(db, session, lm_index, dict(dict_message), rss, )
        await contact.close()
        list_messages.clear()
        match len(rss[1]) > 0:
            case True:
                await update_ids(db=db, tds=tuple(rss[1]), status=Status.sent.value, )
            case _: pass

        match len(rss[2]) > 0:
            case True:
                await update_ids(db=db, tds=tuple(rss[2]), status=Status.failure.value, )
            case _: pass
        print(f"control: {rss[0]}  {len(rss[1])}  {len(rss[2])}")
        return

    @timing_decorator
    async def crms(lc: list, distribution: dict, ) -> list:
        """ to create with concurrent.futures.ThreadPoolExecutor() in future """
        lms = []
        for index_cl, client in enumerate(lc):
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
    
    @timing_decorator
    async def create_queue_messages(db, ld):
        for ld_index, distribution in enumerate(ld):

            async with db.transaction():
                await db.executemany(query_many, await crms(await select(db,
                                                                         table=Table.client.value,
                                                                         fields='id,timezone,phone',
                                                                         model=Client(mob=distribution['mob'],
                                                                                      teg=distribution['teg'], ).dict(),
                                                                         ), distribution, ))

        return

    async def m_restart(db, ):
        restart_status: dict = {1: "'formed', 'queue', 'failure'", 0: "'failure'"}
        return await db.fetch(
             "SELECT d.text, "
             "d.interval, "
             "c.phone, "
             "m.start_date, "
             "m.status, "
             "m.id_distribution, "
             "m.id "
             "FROM client AS c, message AS m, distribution AS d "
             "WHERE ( m.status IN ({query_status}) "    
             "AND d.id = m.id_distribution  "
             "AND c.id = m.id_client ) "
             "ORDER BY m.start_date ; ".format(query_status=restart_status[1]))


    async def seek(db, ):
        return await db.fetch(
            "SELECT d.*, "
            "COUNT(m.status) AS com, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'sent') AS sent, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'queue') AS queue, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'formed') AS formed, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'failure') AS failure, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'expired') AS expired "
            "FROM distribution AS d JOIN message AS m  "
            "ON ( m.id_Distribution=d.id ) "
            "GROUP BY ( d.id   )  "
            "ORDER BY ( d.id ) DESC; "
        )


    """    Begin    """

    locale.setlocale(locale.LC_ALL, "")

    ind, skip = Ind(), '\n'

    app = FastAPI(
        title="API documentation",
        description="A set of Api for completing the task is presented",
        swagger_ui_parameters={"syntaxHighlight.theme": "obsidian"},
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost",
                       "http://localhost:80",
                       "https://localhost:80/docs",
                       "https://localhost:80/docs",
                       "http://localhost:80/client",
                       "http://localhost:80/distributiom",
                       "http://localhost:80/message",
                       "http://localhost:80/admin",
                       "http://localhost:80/admin/statistic",
                       "http://localhost:80/admin/message",
                       "http://localhost:80/admin/message/status",
                       ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    my_middleware = MyMiddleware(some_attribute="")
    app.add_middleware(BaseHTTPMiddleware, dispatch=my_middleware)


    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler():
        return JSONResponse(status_code=400,
                            content=jsonable_encoder([]),
                            )


    conn = db_connect()

    @conn.on_init
    async def initial_db(db):
        with open('create_tables.sql', 'r') as sql:
            return await db.execute(sql.read(), )


    @app.get('/client', status_code=200, description="", )
    async def client_select(response: Response,
                            db=Depends(conn.connection),
                            id: Optional[int] = Query(default=None, ge=0, ),
                            phone: int | None = Query(default=None, ),
                            mob: int | None = Query(default=None, ge=900, le=999, ),
                            teg: str | None = Query(default=None, ),
                            timezone: int | None = Query(default=None, ),
                            ):
        client: Client = Client(id=id,
                                phone=phone,
                                mob=mob,
                                teg=teg,
                                timezone=timezone,
                                )
        match client:
            case (client.id | client.phone | client.mob | client.teg | client.timezone,
                  None):
                model = Client().dict()
            case _: model = json.loads(client.json())
        row = await select(db,
                           table=Table.client.value,
                           model=model,
                           )

        match len(row) == 0:
            case True: response.status_code = 400
            case _: pass
        return row


    @app.post('/client', status_code=400, description="", )
    async def client_insert(response: Response,
                            client: ClientInsert,
                            db=Depends(conn.connection),
                            ):
        row = await insert(db, table=Table.client.value, model=json.loads(client.json()), )
        match len(row) == 1:
            case True:
                response.status_code = 200
            case _:
                pass
        return row


    @app.delete('/client', status_code=200, description="", )
    async def client_delete(response: Response,
                            client: Client,
                            db=Depends(conn.connection),
                            ):
        row = await delete(db, table=Table.client.value, model=json.loads(client.json()), )
        match len(row):
            case 0:
                response.status_code = 400
            case _:
                pass
        return row


    @app.put('/client', status_code=200, description="", )
    async def client_update(response: Response,
                            client: Client,
                            upd: ClientUpdate,
                            db=Depends(conn.connection),
                            ):
        row = await update(db, table=Table.client.value,
                           model=json.loads(client.json()),
                           adu=json.loads(upd.json()),
                           )
        match len(row) == 0:
            case True:
                response.status_code = 400
            case _:
                pass
        return row


    @app.get('/distribution', status_code=200, description="", )
    async def distribution_select(response: Response,
                                  db=Depends(conn.connection, ),
                                  id: int | None = Query(default=None, ge=0, ),
                                  mob: int | None = Query(default=None, ge=900, le=999, ),
                                  teg: str | None = Query(default=None, ),
                                  start_date: datetime | None = Query(default=None, ),
                                  end_date: datetime | None = Query(default=None, ),
                                  text: str | None = Query(default=None, ),
                                  ):
        distribution: Distribution = Distribution(id=id,
                                                  mob=mob,
                                                  teg=teg,
                                                  start_date=start_date,
                                                  end_date=end_date,
                                                  text=text,
                                                  )
        match distribution:
            case (distribution.id | distribution.mob | distribution.teg |
                  distribution.start_date | distribution.end_date,
                  None
                  ):
                model = Distribution().dict()
            case _:
                model = json.loads(distribution.json())
        row = await select(db,
                           table=Table.distribution.value,
                           model=model,
                           )
        match len(row) == 0:
            case True:
                response.status_code = 400
            case _:
                pass
        return row


    @app.post('/distribution', status_code=400, description="", )
    async def distribution_insert(response: Response,
                                  distribution: DistributionInsert,
                                  background_tasks: BackgroundTasks,
                                  db=Depends(conn.connection),
                                  ):
        model = await parsedate(json.loads(distribution.json()))
        match model['end_date'] > model['start_date'] and model['end_date'] > datetime.now():
            case True:
                row = await insert(db,
                                   table=Table.distribution.value,
                                   model=model,
                                   )
                match len(row):
                    case 1:
                        response.status_code = 200
                        background_tasks.add_task(create_queue, db, row, )
                        return row
                    case _:
                        return []
            case _:
                return []


    @app.delete('/distribution', status_code=200, description="", )
    async def delete_distributions(response: Response,
                                   distribution: Distribution,
                                   db=Depends(conn.connection),
                                   ):
        row = await delete(db,
                           table=Table.distribution.value,
                           model=await parsedate(json.loads(distribution.json())),
                           )
        match len(row):
            case 0:
                response.status_code = 400
            case _:
                pass
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
            case True: return []
            case _: pass
        row = await update(db, table=Table.distribution.value,
                           model=await parsedate(json.loads(distribution.json()), ),
                           adu=adu,
                           )
        match len(row):
            case 0: return []
            case _:
                response.status_code = 200
                background_tasks.add_task(
                                          create_queue,
                                          db,
                                          row,
                                          )
                return row


    @app.get('/message', status_code=400, description="", )
    async def select_message(response: Response,
                             db=Depends(conn.connection),
                             id: int | None = Query(default=None, ge=0, ),
                             id_distribution: int | None = Query(default=None, ge=0, ),
                             id_client: int | None = Query(default=None, ge=0, ),
                             status: Status | None = Query(default=None, ),
                             start_date: datetime | None = Query(default=None, ),
                             ):

        message = Message(id=id,
                          id_distribution=id_distribution,
                          id_client=id_client,
                          start_date=start_date,
                          status=status,
                          )
        match message:
            case (message.id | message.id_distribution | message.id_client | message.start_date | message.status,
                  None
                  ): model = Message().dict()
            case _: model = json.loads(message.json())
        row = await select(db,
                           table=Table.message.value,
                           model=model,
                           )
        match len(row) >= 1:
            case True:
                response.status_code = 200
                return row
            case _: return []


    """  next script for Web UI(admin)    """


    @app.get("/")
    async def main():
        return FileResponse("data.html")


    @app.get('/admin/distribution', status_code=200, description="", )
    async def select_distributions(db=Depends(conn.connection), ):
        return await seek(db, )


    @app.get('/admin/statistic', status_code=200, description="", )
    async def select_distribution_by_id(db=Depends(conn.connection),
                                        id: int = Query(ge=0, ),
                                        ):
        return await db.fetch(
                 "SELECT d.*, COUNT(m.status) AS com, "   
                 "COUNT(m.status) FILTER (WHERE  m.status = 'sent') AS sent, "
                 "COUNT(m.status) FILTER (WHERE  m.status = 'queue') AS queue, "
                 "COUNT(m.status) FILTER (WHERE  m.status = 'formed') AS formed, "
                 "COUNT(m.status) FILTER (WHERE  m.status = 'failure') AS failure, "
                 "COUNT(m.status) FILTER (WHERE  m.status = 'expired') AS expired "
                 "FROM Distribution AS d JOIN Message as m  "
                 "ON ( d.id={id} AND m.id_Distribution={id} ) "
                 "GROUP BY ( d.id   )  ".format(id=id),
        )


    @app.get('/admin/message', status_code=200, description="", )
    async def select_messages(db=Depends(conn.connection),
                              id_distribution: int = Query(ge=0, ),
                              ):
        return await db.fetch(
                 "SELECT m.*, "
                 "c.timezone, "
                 "c.phone "
                 "FROM message AS m, client as c "
                 "WHERE (m.id_distribution={id} AND "
                 "c.id=m.id_client ) "
                 "ORDER BY  m.start_date, c.timezone, c.phone, m.status ;".format(id=id_distribution),
        )


    @app.get('/admin/message/status', status_code=200, description="", )
    async def select_messages_status(db=Depends(conn.connection),
                                     id_distribution: int = Query(ge=0, ),
                                     status: str = Query(),
                                     ):
        return await db.fetch(
                 "SELECT m.*, "
                 "c.timezone AS timezone, "
                 "c.phone AS phone "
                 "FROM message AS m, client as c "
                 "WHERE (m.id_distribution={id} AND "
                 "m.status='{status}' AND "
                 "c.id=m.id_client ) "
                 "ORDER BY m.start_date, c.timezone, c.phone ;".format(id=id_distribution,
                                                                       status=status,
                                                                       ),
        )


except (Exception,  ValueError, TypeError, ) as e:
    logging.info(f"Basis error: {e}")
    pass

except HTTPException as e:
    logging.info(f"HTTP error: {e}")
    pass

except exceptions as e:
    logging.info(f"Rabbitmq error: {e}")
    pass

except asyncpg_exception as e:
    logging.info(f"Asyncpg error: {e}")
    pass

finally:
    pass


if __name__ == "__main__":

    try:
        uvicorn.run('mod:app', host='0.0.0.0', port=80, )  # reload=True, )
    except KeyboardInterrupt:
        exit()
