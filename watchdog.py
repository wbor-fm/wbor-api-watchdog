"""
"Watch" for new spins on a Spinitron API proxy and publish them to
RabbitMQ. Listens for SSE updates to indicate a new spin is available,
falling back to polling if SSE becomes unavailable.

Author: Mason Daugherty <@mdrxy>
Version: 1.1.0
Last Modified: 2025-04-15

Changelog:
    - 1.0.0 (2025-03-23): Initial release.
    - 1.1.0 (2025-04-15): Fixes and enhancements to RabbitMQ connection
        management, error handling, and state management.
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time

import aio_pika
import aiohttp
from aiosseclient import aiosseclient
from dotenv import load_dotenv

from utils.logging import configure_logging

logging.root.handlers = []
logger = configure_logging()

load_dotenv()

# Load environment variables (RabbitMQ credentials, API endpoint)
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY")

API_BASE_URL = os.getenv("API_BASE_URL")
SSE_STREAM_URL = f"{API_BASE_URL}/spin-events"
SPIN_GET_URL = f"{API_BASE_URL}/api/spins"
logger.debug("SSE_STREAM_URL: `%s`", SSE_STREAM_URL)
logger.debug("SPIN_GET_URL: `%s`", SPIN_GET_URL)

# Configurable retry and circuit breaker parameters
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))
RETRY_SSE_INTERVAL = int(os.getenv("RETRY_SSE_INTERVAL", "300"))
CB_ERROR_THRESHOLD = int(os.getenv("CB_ERROR_THRESHOLD", "5"))
CB_RESET_TIMEOUT = int(os.getenv("CB_RESET_TIMEOUT", "60"))

# Persistent connections
HTTP_SESSION = None
RABBITMQ_PUBLISHER = None


class RabbitMQPublisher:  # pylint: disable=too-many-instance-attributes
    """
    Object to manage RabbitMQ connection and publishing messages.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self, host, user, password, exchange_name, routing_key
    ):
        self.host = host
        self.user = user
        self.password = password
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        """
        Connect to RabbitMQ and declare the exchange (if it doesn't
        exist).
        """
        self.connection = await aio_pika.connect_robust(
            host=self.host, login=self.user, password=self.password
        )
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )

    async def publish(self, spin_data):
        """
        Publish spin data to RabbitMQ.
        """
        message = aio_pika.Message(body=json.dumps(spin_data).encode("utf-8"))
        await self.exchange.publish(message, routing_key=self.routing_key)
        logger.info(
            "Published spin data to RabbitMQ on `%s` with key `%s`: `%s - %s`",
            self.exchange_name,
            self.routing_key,
            spin_data.get("artist"),
            spin_data.get("song"),
        )

    async def close(self):
        """
        Close the RabbitMQ connection and channel.
        """
        if self.connection:
            await self.connection.close()


NEW_SPIN_EVENT = "new spin data"

if not all(
    [
        RABBITMQ_HOST,
        RABBITMQ_USER,
        RABBITMQ_PASS,
        RABBITMQ_QUEUE,
        RABBITMQ_EXCHANGE,
        RABBITMQ_ROUTING_KEY,
        API_BASE_URL,
    ]
):
    logger.critical("Missing required environment variables.")
    sys.exit(1)

shutdown_event = asyncio.Event()


def handle_shutdown(signum, _frame):
    """
    Handle shutdown signals (SIGINT, SIGTERM).
    """
    logger.info("Received shutdown signal: %s", signum)
    shutdown_event.set()


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class SpinState:
    """
    Track the last spin ID to avoid duplicates.
    """

    def __init__(self):
        self.last_spin_id = None


SPIN_STATE = SpinState()


async def process_spin(spin, state):
    """
    Check if the spin is new (by comparing spin IDs). If it is,
    update the last_spin_id and publish the spin data.
    """
    spin_id = spin.get("id")
    if spin_id != state.last_spin_id:
        state.last_spin_id = spin_id
        await send_to_rabbitmq(spin)
    else:
        logger.debug("Duplicate spin received (ID: `%s`). Skipping publish.", spin_id)


async def sse_is_reachable():
    """
    Simple helper function to check if the SSE endpoint is responding.
    """
    try:
        async with HTTP_SESSION.get(SSE_STREAM_URL, timeout=5) as response:
            return response.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.debug("SSE reachability check failed: %s", e)
        return False


async def fetch_latest_spin():
    """
    Fetch the latest spin from the API.
    """
    async with HTTP_SESSION.get(SPIN_GET_URL, timeout=10) as response:
        if response.status == 200:
            spins_data = await response.json()

            # Directly fetch item 0 as it is the latest spin
            items = spins_data.get("items")
            if items:
                latest_spin = items[0]
                logger.debug("Latest spin: `%s`", latest_spin)
                return latest_spin
            logger.warning("No spins found in response.")
            return None
        logger.critical("Failed to fetch spin data: `%s`", response.status)
        return None


async def listen_to_sse(state):
    """
    Connect to the SSE stream and listen for new spin messages.
    When a new spin message is received, fetch the latest spin data and
    publish it to RabbitMQ. On repeated failures, fall back to polling.
    """
    logger.debug("Listening for SSE at: %s", SSE_STREAM_URL)
    circuit_breaker_failure_count = 0

    retry_count = 0

    while not shutdown_event.is_set():
        if circuit_breaker_failure_count >= CB_ERROR_THRESHOLD:
            logger.error(
                "Circuit breaker triggered. Pausing SSE connection attempts for %d seconds.",
                CB_RESET_TIMEOUT,
            )
            await asyncio.sleep(CB_RESET_TIMEOUT)
            circuit_breaker_failure_count = 0
            retry_count = 0
            continue

        try:
            # Connect to SSE in a streaming fashion
            if retry_count == 0:
                logger.info("Connecting to SSE...")
            else:
                logger.info("Connecting to SSE (try #%d)...", retry_count + 1)
            async for event in aiosseclient(SSE_STREAM_URL):
                if shutdown_event.is_set():
                    logger.info("Shutting down SSE listener.")
                    break

                if retry_count > 0:
                    logger.info(
                        "Successfully reconnected to SSE endpoint at %s", SSE_STREAM_URL
                    )
                    retry_count = 0

                if event.data == NEW_SPIN_EVENT:
                    logger.debug("Received SSE event: `%s`", event.data)
                    spin = await fetch_latest_spin()
                    if spin is not None:
                        await process_spin(spin, state)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if shutdown_event.is_set():
                break

            circuit_breaker_failure_count += 1
            retry_count += 1
            logger.warning(
                "SSE connection dropped or failed (attempt #%d) for URL %s: %s",
                retry_count,
                SSE_STREAM_URL,
                e,
            )

            if retry_count > MAX_RETRIES:
                logger.warning(
                    "SSE attempts exceeded %d. Switching to polling fallback.",
                    MAX_RETRIES,
                )
                await poll_for_spins(state)
                return

            # Sleep briefly before the next reconnect attempt with exponential backoff
            backoff = min(2**retry_count + random.uniform(0, 1), 30)
            await asyncio.sleep(backoff)


async def poll_for_spins(
    state, poll_interval=POLL_INTERVAL, retry_sse_interval=RETRY_SSE_INTERVAL
):
    """
    Poll for new spins at a fixed interval. Periodically check if SSE is
    available again, and if so, return to allow the SSE listener to
    resume.

    Parameters:
    - poll_interval: Interval in seconds to poll for new spins.
    - retry_sse_interval: Interval in seconds to check if SSE is
        available again and return to SSE if it is.
    """
    logger.info("Polling for new spins as a fallback...")
    time_of_last_sse_check = time.time()

    while not shutdown_event.is_set():
        try:
            # Poll for a new spin
            current_spin = await fetch_latest_spin()
            if current_spin is not None:
                await process_spin(current_spin, state)

            # Periodically attempt to see if SSE is available
            if time.time() - time_of_last_sse_check >= retry_sse_interval:
                time_of_last_sse_check = time.time()
                if await sse_is_reachable():
                    logger.info("SSE appears available again. Returning to SSE.")
                    return

            await asyncio.sleep(poll_interval)

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            aio_pika.exceptions.AMQPError,
        ) as e:
            logger.error("Error during polling: %s", e, exc_info=True)
            await asyncio.sleep(poll_interval)


async def send_to_rabbitmq(spin_data):
    """
    Publish spin data to RabbitMQ using the persistent connection.
    """
    try:
        await RABBITMQ_PUBLISHER.publish(spin_data)
    except (aio_pika.exceptions.AMQPError, json.JSONDecodeError) as e:
        logger.critical("Error publishing to RabbitMQ: `%s`", e)


async def main():
    """
    Entry point for the script.

    Initializes persistent HTTP session and RabbitMQ publisher, then
    starts the SSE listener.
    """
    global HTTP_SESSION, RABBITMQ_PUBLISHER  # pylint: disable=global-statement
    logger.info("Starting WBOR Spinitron watchdog...")

    # Initialize persistent HTTP session and RabbitMQ publisher
    HTTP_SESSION = aiohttp.ClientSession()
    RABBITMQ_PUBLISHER = RabbitMQPublisher(
        RABBITMQ_HOST,
        RABBITMQ_USER,
        RABBITMQ_PASS,
        RABBITMQ_EXCHANGE,
        RABBITMQ_ROUTING_KEY,
    )
    await RABBITMQ_PUBLISHER.connect()

    try:
        await listen_to_sse(SPIN_STATE)
    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        aio_pika.exceptions.AMQPError,
    ) as e:
        logger.critical("Unhandled exception in main: %s", e, exc_info=True)
    finally:
        logger.info("Shutting down connections...")
        await HTTP_SESSION.close()
        await RABBITMQ_PUBLISHER.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
        shutdown_event.set()
