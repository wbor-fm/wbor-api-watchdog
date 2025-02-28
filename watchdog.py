"""
"Watch" for new spins on a Spinitron API proxy and publish them to
RabbitMQ. Listens for SSE updates to indicate a new spin is available,
falling back to polling if SSE becomes unavailable.

Author: Mason Daugherty <@mdrxy>
Version: 1.0.0
Last Modified: 2025-03-23

Changelog:
    - 1.0.0 (2025-03-23): Initial release.
"""

import asyncio
import json
import logging
import os
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


async def sse_is_reachable():
    """
    Simple helper function to check if the SSE endpoint is responding.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(SSE_STREAM_URL, timeout=5) as response:
                return response.status == 200
    except Exception as e:  # pylint: disable=broad-except
        logger.debug("SSE reachability check failed: %s", e)
        return False


async def fetch_latest_spin():
    """Fetch the latest spin from the API."""
    async with aiohttp.ClientSession() as session:
        async with session.get(SPIN_GET_URL) as response:
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


async def listen_to_sse():
    """
    Connect to the SSE stream and listen for new spin messages.

    When a new spin message is received, fetch the latest spin data and
    publish it to RabbitMQ.
    """
    logger.debug("Listening for SSE at: %s", SSE_STREAM_URL)

    # We'll use a loop to reconnect if SSE fails
    while not shutdown_event.is_set():
        retry_count = 0
        try:
            async for event in aiosseclient(SSE_STREAM_URL):
                if shutdown_event.is_set():
                    logger.info("Shutting down SSE listener.")
                    break

                if event.data == NEW_SPIN_EVENT:
                    logger.debug("Received SSE event: `%s`", event.data)
                    spin = await fetch_latest_spin()
                    if spin is not None:
                        await send_to_rabbitmq(spin)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if shutdown_event.is_set():
                break
            retry_count += 1
            if retry_count > 5:
                logger.error("SSE attempts exceeded 5, switching to polling fallback.")
                await poll_for_spins()
                return
            logger.error("SSE connection dropped or failed: %s", e)
            await asyncio.sleep(5)

        except Exception as e:  # pylint: disable=broad-except
            if shutdown_event.is_set():
                break
            logger.critical("Unexpected error in SSE loop: %s", e, exc_info=True)
            await asyncio.sleep(5)


async def poll_for_spins(poll_interval=3, retry_sse_interval=300):
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
    last_spin_id = None
    time_of_last_sse_check = time.time()

    while not shutdown_event.is_set():
        try:
            # Poll for a new spin
            current_spin = await fetch_latest_spin()
            if current_spin is not None:
                spin_id = current_spin.get("id")
                if spin_id != last_spin_id:
                    last_spin_id = spin_id
                    await send_to_rabbitmq(current_spin)

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
    """Publish spin data to RabbitMQ."""
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST, login=RABBITMQ_USER, password=RABBITMQ_PASS
        )
        channel = await connection.channel()

        # Declare the durable topic exchange
        exchange = await channel.declare_exchange(
            RABBITMQ_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )

        # Publish the message
        message = aio_pika.Message(body=json.dumps(spin_data).encode("utf-8"))
        await exchange.publish(message, routing_key=RABBITMQ_ROUTING_KEY)

        logger.info(
            "Published spin data to RabbitMQ on `%s` with key `%s`: `%s - %s`",
            RABBITMQ_EXCHANGE,
            RABBITMQ_ROUTING_KEY,
            spin_data.get("artist"),
            spin_data.get("song"),
        )

        await connection.close()
    except (
        aio_pika.exceptions.AMQPConnectionError,
        aio_pika.exceptions.ChannelClosed,
    ) as e:
        logger.critical("Error publishing to RabbitMQ: `%s`", e)


async def main():
    """
    Entry point for the script.

    This function listens to the SSE stream and triggers updates when a
    new spin is available. If SSE fails repeatedly, it falls back to
    polling, which can return to SSE if it becomes available again.
    """
    logger.info("Starting WBOR Spinitron watchdog...")

    try:
        await listen_to_sse()
    except (
        aiohttp.ClientError,
        asyncio.TimeoutError,
        aio_pika.exceptions.AMQPError,
    ) as e:
        logger.critical("Unhandled exception in main: %s", e, exc_info=True)
    finally:
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested.")
    finally:
        shutdown_event.set()
        loop.run_until_complete(asyncio.sleep(0.1))  # Allow cleanup tasks
        loop.close()
