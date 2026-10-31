import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID, uuid4

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
COMMANDS_TOPIC = "device.commands"
RESULTS_TOPIC = "device.command-results"
COMMAND_TTL = timedelta(seconds=60)
# Time for the result of an expired command to arrive
TIMEOUT_GRACE = timedelta(seconds=5)
# The monolith does not store protocols yet
DEFAULT_PROTOCOL = "MQTT"

logger = logging.getLogger(__name__)


class Action(str, Enum):
    TURN_ON = "TURN_ON"
    TURN_OFF = "TURN_OFF"


class CommandBody(BaseModel):
    action: Action


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        self.status_code = status_code
        self.code = code
        self.message = message


class MonolithClient:
    """Reads and switches heating devices stored in the monolith."""

    def __init__(self, base_url: str):
        self._base_url = base_url

    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        try:
            async with httpx.AsyncClient(base_url=self._base_url, timeout=5) as client:
                return await client.request(method, path, **kwargs)
        except httpx.HTTPError as error:
            raise ApiError(503, "MONOLITH_UNAVAILABLE", "Монолит недоступен") from error

    async def get_heating_device(self, device_id: str) -> dict:
        response = await self._request("GET", f"/api/v1/sensors/{device_id}")
        # The monolith answers 400 to an identifier that is not a number
        if response.status_code in (400, 404):
            raise ApiError(404, "DEVICE_NOT_FOUND", "Устройство не найдено")
        if response.status_code != 200:
            raise ApiError(503, "MONOLITH_UNAVAILABLE", "Монолит недоступен")
        device = response.json()
        if device["type"] != "heating":
            raise ApiError(409, "DEVICE_TYPE_NOT_SUPPORTED", "Устройство не относится к отоплению")
        return device


class CommandBus:
    """Publishes commands to Kafka and tracks their results in memory."""

    def __init__(self, producer=None):
        self.producer = producer
        self.commands: dict[UUID, dict] = {}

    async def send(self, device_id: str, action: Action) -> dict:
        now = datetime.now(timezone.utc)
        command = {
            "id": uuid4(),
            "device_id": device_id,
            "action": action,
            "status": "PENDING",
            "expires_at": now + COMMAND_TTL,
            "created_at": now,
            "finished_at": None,
        }
        message = {
            "command_id": str(command["id"]),
            "device_id": device_id,
            "protocol": DEFAULT_PROTOCOL,
            "action": action.value,
            "params": {},
            "expires_at": command["expires_at"].isoformat(),
        }
        self.commands[command["id"]] = command
        try:
            await self.producer.send_and_wait(
                COMMANDS_TOPIC, value=json.dumps(message).encode(), key=device_id.encode()
            )
        except KafkaError as error:
            del self.commands[command["id"]]
            raise ApiError(503, "BROKER_UNAVAILABLE", "Брокер сообщений недоступен") from error
        return command

    def get(self, command_id: UUID) -> dict:
        command = self.commands.get(command_id)
        if command is None:
            raise ApiError(404, "COMMAND_NOT_FOUND", "Команда не найдена")
        now = datetime.now(timezone.utc)
        if command["status"] == "PENDING" and now > command["expires_at"] + TIMEOUT_GRACE:
            command["status"] = "TIMED_OUT"
            command["finished_at"] = now
        return command

    def apply_result(self, raw: bytes) -> None:
        """Finishes a pending command. Results of other services and repeated results are ignored."""
        try:
            result = json.loads(raw)
            command = self.commands.get(UUID(result["command_id"]))
            if command is not None and command["status"] == "PENDING":
                command["finished_at"] = datetime.fromisoformat(result["occurred_at"])
                command["status"] = result["status"]
        except (ValueError, KeyError, TypeError) as error:
            logger.warning("Skipping invalid command result %r: %s", raw, error)


async def consume_results(consumer: AIOKafkaConsumer) -> None:
    async for message in consumer:
        bus.apply_result(message.value)


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    consumer = AIOKafkaConsumer(
        RESULTS_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id="heating-service",
        auto_offset_reset="earliest",
    )
    await producer.start()
    await consumer.start()
    bus.producer = producer
    task = asyncio.create_task(consume_results(consumer))
    yield
    task.cancel()
    await consumer.stop()
    await producer.stop()


app = FastAPI(title="Heating API", lifespan=lifespan)
monolith = MonolithClient(MONOLITH_URL)
bus = CommandBus()


@app.exception_handler(ApiError)
async def handle_api_error(request: Request, error: ApiError):
    return JSONResponse({"code": error.code, "message": error.message}, status_code=error.status_code)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/v1/heating/devices/{device_id}/commands", status_code=202)
async def create_command(device_id: str, body: CommandBody):
    await monolith.get_heating_device(device_id)
    return await bus.send(device_id, body.action)


@app.get("/api/v1/heating/commands/{command_id}")
async def get_command(command_id: UUID):
    return bus.get(command_id)


@app.get("/api/v1/heating/devices/{device_id}/state")
async def get_state(device_id: str):
    device = await monolith.get_heating_device(device_id)
    reported = Action.TURN_ON if device["status"] == "active" else Action.TURN_OFF
    return {"device_id": device_id, "reported": reported}
