
json = __import__('orjson')
from  asyncio import run_coroutine_threadsafe
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from loguru import logger
from uvicorn import run


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.main_loop = asyncio.get_running_loop()
    yield
    manager.active_connections.clear()


app = FastAPI(lifespan=lifespan)
logger.remove()
logger.add("app.json", 
           serialize=True, 
           rotation="10 MB", 
           retention="1 day", )

def websocket_log_sink(message):
    record = message.record
    log_data = {
        "time": record["time"].strftime("%H:%M:%S"),
        "level": record["level"].name,
        "message": record["message"],
    }
    try:
        loop = app.state.main_loop
        if loop and loop.is_running():
            run_coroutine_threadsafe(
                manager.broadcast(json.dumps(log_data)), loop
            )
    except (AttributeError, RuntimeError):
        logger.info("AttributeError, RuntimeError 58")
        pass

logger.add(websocket_log_sink)

@app.get("/", include_in_schema=False)
async def main():
    return FileResponse("log.html")

@app.websocket("/ws/logs")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    logger.info("The browser has successfully connected to the WebSocket logs!")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/generate-log/{level}")
async def generate_log(level: str, text: str = "Тестовое сообщение"):
    if level == "info":
        logger.info(text)
    elif level == "warning":
        logger.warning(text)
    elif level == "error":
        logger.error(text)
    return {"status": f"Log {level.upper()} created"}


if __name__ == "__main__":
    try:
        run(
            app,
            host='0.0.0.0',
            port=80,
            log_level="warning"
        )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt: the end of task")
