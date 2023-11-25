# -*- coding: utf-8 -*-
""" script by js18user  """
import asyncio
import logging
import aiohttp
import locale
import os
import time
import uvicorn

from datetime import datetime
from enum import Enum
from typing import Union
from functools import wraps
from asyncpg import exceptions
import orjson as json
from dateutil.parser import parse
from fastapi import FastAPI, Depends, BackgroundTasks, Query, Response, HTTPException
from fastapi.responses import FileResponse
from fastapi_asyncpg import configure_asyncpg
from pydantic import BaseModel, Field
from create_tables import headers


class Ind(BaseModel):
    index: int = 0
    upd: int = 0
    sel: int = 0
    name: str = 'for background task'
    interval = datetime.now() - datetime.utcnow()
    ine = parse(f'1:0:0') - parse(f'0:0:0')
    api: bool = None
    restart: int = 1


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
        logging.info(f"Function {func_async.__name__} took {int((time.time() - start_time)*1000)} m.sec")
        return result
    return wrapper


def memoize(f):
    cache = dict()

    async def wrapper(x):
        if x not in cache:
            cache[x] = await f(x)
        return cache[x]
    return wrapper


try:
    def db_connect():

        cone = configure_asyncpg(app, 'postgresql://{user}:{password}@{host}:{port}/{name}'.format(
            user='postgres',
            name='fintech',
            password='aa4401',
            port=5432,
            host='localhost'))
        return cone


    async def db_connection():
        """  Reserved for future  """

        connect = configure_asyncpg(app, 'postgresql://{user}:{password}@{host}:{port}/{name}'.format(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            port=os.environ['DB_PORT'],
            name=os.environ['DB_NAME']))
        return connect


    async def realtime(self, zone):
        return self + ind.interval - ind.ine*zone


    async def parsedate(model):
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
        counter, params, cond, vals, qs = 1, [], [], [], "update {table} set {columns} where {cond} returning * "

        for column, value in model.items():
            if value is not None and value != 0:
                cond.append(f"{column}=${counter}")
                params.append(value)
                counter += 1
            else:
                pass
        for column, value in adu.items():
            if value is not None and value != 0:
                vals.append(f"{column}=${counter}")
                params.append(value)
                counter += 1
            else:
                pass

        sql = qs.format(
            table=table, columns=" ,".join(vals), cond=" AND ".join(cond)
        )
        return sql, params


    def query_delete(table, model, fields='*', ):
        if model.get('id') is None:
            return "delete from {table}  where  {where}  returning {fields} ;".format(
                table=table,
                fields=fields,
                where=(" and ".join(["%s='%s'" % (item, model[item])
                                    for item in model.keys()
                                    if (model[item] is not None)
                                     ])),
            )
        else:
            return f"delete from {table}  where id = {model.get('id')}  returning {fields} ;"


    def query_select(table, model, fields="*"):
        # print(model)
        if model.get('id') is None:
            where = " and ".join(["%s='%s'" % (item, model[item]) for item in model.keys() if (model[item] is not None)]
                                 )
            if where == "":
                return f"select {fields} from {table}  "
            else:
                return f"select {fields} from {table} where ({where}) ;"
        else:
            return f"select {fields} from {table}  where id = {model.get('id')};"


    def query_insert(table, model, fields="*"):
        length_model = len(model.values())
        return "Insert Into {table} ({columns}) Values ({values}) On Conflict Do Nothing Returning {fields};".format(
            table=table,
            values=",".join([f"${p + 1}" for p in range(length_model)]),
            columns=",".join(list(model.keys())),
            fields=fields,
        )


    async def insert(db, table, model, ):
        async with db.transaction():
            return await db.fetch(query_insert(table, model), *list(model.values()), )


    async def select(db, table, model, args=None, fields="*"):
        return await db.fetch(query_select(table, model, fields, ), *(args or []), )


    async def delete(db, table, model, args=None, fields='*', ):
        async with db.transaction():
            return await db.fetch(query_delete(table, model, fields), *(args or []), )


    async def update(db, table, model, adu):
        sql, params = query_update(table, model, adu, )
        async with db.transaction():
            return await db.fetch(sql, *params)


    async def send_message(db, session, dict_message, ):
        dict_message['status'] = Status.queue

        await update(db, table=Table.message,
                     model=Message(id=dict_message['id']).dict(),
                     adu=MessageUpdate(status=dict_message['status'], ).dict(),
                     )

        pause: int = ((dict_message['start_date']).timestamp().__sub__(datetime.now().timestamp()))
        if pause < 0:
            pause = 0
        else:
            pass
        await asyncio.sleep(pause, )

        async with session.post(f'https://probe.fbrq.cloud/v1/send/{dict_message["id"]}',
                                headers=headers,
                                json={
                                      'id': dict_message['id'],
                                      'phone': dict_message['phone'],
                                      'text': dict_message['text'],
                                       }
                                ) as resp:
            response = await resp.json()

        if (response['code'] == 0) and (response['message'] == 'OK') and (resp.status == 200):
            dict_message['status'] = Status.sent
        else:
            dict_message['status'] = Status.failure
            ind.restart = 0

        await update(db, table=Table.message,
                     model=Message(id=dict_message['id']).dict(),
                     adu=MessageUpdate(status=dict_message['status'],
                                       start_date=datetime.now()
                                       ).dict(),
                     )

        logging.info(f"{resp.status} {response} {dict_message['start_date']}", )
        return


    async def create_queue(db, list_distributions, ):

        await create_queue_release(db, list_messages=await restart(db, ), )
        await create_queue_release(db, list_messages=await create_queue_messages(db, list_distributions, ), )
        return()


    async def create_queue_release(db, list_messages, ):
        """ Where is None: current operation; Where is True: restart operation """

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0, use_dns_cache=False, ),
                                         read_bufsize=2 ** 10,
                                         ) as session:

            for dict_message in list_messages:
                match dict_message['status']:
                    case Status.expired:
                        pass
                    case _:
                        try:
                            await send_message(db, session, dict_message)
                        except (Exception,
                                ValueError,
                                TypeError,
                                ) as err:
                            logging.info(f"There are problems with aiohttp.post {skip} error: {err}", )
                            dict_message['status'] = Status.failure
                            ind.restart = 0
                            await update(db, table=Table.message,
                                         model=Message(id=dict_message['id']).dict(),
                                         adu=MessageUpdate(status=dict_message['status'],
                                                           start_date=datetime.now()).dict(),
                                         )
                        else:
                            pass

        return


    @timing_decorator
    async def create_queue_messages(db, list_distributions):
        list_messages: list = []
        for distribution in list_distributions:

            list_clients: list = await select(db,
                                              table=Table.client,
                                              fields='id,timezone,phone',
                                              model=Client(
                                                           mob=distribution['mob'],
                                                           teg=distribution['teg'],
                                                           ).dict(),
                                              )
            list_mess: list = []
            for client in list_clients:
                if await realtime(distribution['end_date'], client['timezone']) < datetime.now():
                    status = Status.expired
                else:
                    status = Status.formed
                model = MessageInsert(status=status,
                                      start_date=parse(str(await realtime(distribution['start_date'],
                                                                          client['timezone']))),
                                      id_distribution=distribution['id'],
                                      id_client=client['id'],
                                      ).dict()
                list_mess.append(model.values())

                message_one = await insert(db,
                                           table=Table.message,
                                           model=model,
                                           )

                dict_one: dict = {'text': distribution['text'],
                                  'interval': distribution['interval'],
                                  'phone': client['phone']
                                  }
                dict_one.update(dict(message_one[0]))
                list_messages.append(dict_one)
        list_messages.sort(key=lambda messages: messages['start_date'])
        return list_messages


    async def restart(db, ):
        query_status: dict = {1: "'formed', 'queue', 'failure'", 0: "'failure'"}
        logging.info(f"query_status: {query_status.get(ind.restart)}")

        query = ("SELECT d.Text,"
                 "d.interval,"
                 "c.phone,"
                 "m.start_date,"
                 "m.Status,"
                 "m.Id_Distribution,"
                 "m.id "
                 "FROM client AS c, message AS m, distribution AS d "
                 "WHERE ( m.status IN ({query_status}) "    
                 "AND d.id = m.id_Distribution  "
                 "AND c.id = m.id_Client ) "
                 "ORDER BY m.start_date ; ".format(query_status=query_status.get(ind.restart)))
        list_messages, ind.restart = list(map(dict, await db.fetch(query))), 0
        return list_messages


    async def seek(db, ):
        return await db.fetch(
            "SELECT d.*, "
            "COUNT(m.status) AS com, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'sent') AS sent, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'queue') AS queue, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'formed') AS formed, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'failure') AS failure, "
            "COUNT(m.status) FILTER (WHERE  m.status = 'expired') AS expired "
            "FROM Distribution AS d JOIN Message as m  "
            "ON ( m.id_Distribution=d.id ) "
            "GROUP BY ( d.id   )  "
            "ORDER BY ( d.id ) DESC; "
        )


    """ ........................................   Let's go....    .............................."""

    locale.setlocale(locale.LC_ALL, "")
    logging.basicConfig(level=logging.INFO,
                        datefmt='%m-%d-%Y %I:%M:%S %p',
                        format=f"%(asctime)s :%(process)d :%(funcName)s: :%(lineno)d: %(message)s",
                        )
    ind, skip = Ind(), '\n'
    app = FastAPI(
        title="API documentation",
        description="A set of Api for completing the task is presented",
    )
    conn = db_connect()


    @conn.on_init
    async def initial_db(db):
        with open('create_tables.sql', 'r') as sql:
            return await db.execute(sql.read(), )


    @app.get('/client', status_code=200, description="", )
    async def client_select(response: Response,
                            db=Depends(conn.connection),
                            id: int | None = Query(default=None, ge=0, ),
                            phone: int | None = Query(default=None, ),
                            mob: int | None = Query(default=None, ge=900, le=999, ),
                            teg: str | None = Query(default=None, ),
                            timezone: int | None = Query(default=None, ),
                            ):
        client = Client(id=id,
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
                           table=Table.client,
                           model=model,
                           )
        if len(row) == 0:
            response.status_code = 400
        else:
            pass
        return row


    @app.post('/client', status_code=400, description="", )
    async def client_insert(response: Response,
                            client: ClientInsert,
                            db=Depends(conn.connection),
                            ):
        row = await insert(db, table=Table.client, model=json.loads(client.json()), )
        if len(row) == 1:
            response.status_code = 200
        else:
            pass
        return row


    @app.delete('/client', status_code=200, description="", )
    async def client_delete(response: Response,
                            client: Client,
                            db=Depends(conn.connection),
                            ):
        row = await delete(db, table=Table.client, model=json.loads(client.json()), )
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
        row = await update(db, table=Table.client,
                           model=json.loads(client.json()),
                           adu=json.loads(upd.json()),
                           )
        if len(row) == 0:
            response.status_code = 400
        else:
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

        distribution = Distribution(id=id,
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
                           table=Table.distribution,
                           model=model,
                           )
        if len(row) == 0:
            response.status_code = 400
        else:
            pass
        return row


    @app.post('/distribution', status_code=400, description="", )
    async def distribution_insert(response: Response,
                                  distribution: DistributionInsert,
                                  background_tasks: BackgroundTasks,
                                  db=Depends(conn.connection),
                                  ):
        model = await parsedate(json.loads(distribution.json()))
        if model['end_date'] > model['start_date'] and model['end_date'] > datetime.now():
            row = await insert(db,
                               table=Table.distribution,
                               model=model,
                               )
            match len(row):
                case 1:
                    response.status_code = 200
                    background_tasks.add_task(create_queue, db, row, )
                    return row
                case _:
                    return []
        else:
            return []


    @app.delete('/distribution', status_code=200, description="", )
    async def delete_distributions(response: Response,
                                   distribution: Distribution,
                                   db=Depends(conn.connection),
                                   ):
        row = await delete(db,
                           table=Table.distribution,
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
        if adu['end_date'] < datetime.now():
            return []
        row = await update(db, table=Table.distribution,
                           model=await parsedate(json.loads(distribution.json()), ),
                           adu=adu,
                           )
        logging.debug(f"update info: {len(row)}")
        match len(row):
            case 0:
                return []
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
                  ):
                model = Message().dict()
            case _:
                model = json.loads(message.json())
        row = await select(db,
                           table=Table.message,
                           model=model,
                           )
        if len(row) >= 1:
            response.status_code = 200
            return row
        else:
            return []


    """  next script for Web UI(admin)    """


    @app.get("/")
    async def main():
        return FileResponse("data.html")


    @app.get('/admin/distribution', status_code=200, description="", )
    async def select_distributions(db=Depends(conn.connection), ):
        return await seek(db, )


    @app.get('/admin/statistic', status_code=200, description="", )
    async def select_distribution_by_id(db=Depends(conn.connection),
                                        ids: int = Query(ge=0, ),
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
                 "GROUP BY ( d.id   )  ".format(id=ids),
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
                                     status: Status = Query(),
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

except (Exception,
        ValueError,
        exceptions,
        HTTPException,
        ) as error_message:
    logging.info(f"Com error: {error_message}")
    pass
else:
    pass

if __name__ == "__main__":

    try:
        uvicorn.run('main:app', host='0.0.0.0', port=8000, )  # reload=True, )
    except KeyboardInterrupt:
        exit()
