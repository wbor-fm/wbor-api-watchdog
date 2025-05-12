"""
"Watch" for new spins on a Spinitron API proxy and publish them to
RabbitMQ. Listens for SSE updates to indicate a new spin is available,
falling back to polling if SSE becomes unavailable. If the proxy API
for fetching spins is down, it attempts to use the primary Spinitron 
API.

Author: Mason Daugherty <@mdrxy>
Version: 1.2.0
Last Modified: 2025-05-12

Changelog:
    - 1.0.0 (2025-03-23): Initial release.
    - 1.1.0 (2025-04-15): Fixes and enhancements to RabbitMQ connection
        management, error handling, and state management.
    - 1.2.0 (2025-05-12): Added fallback to primary Spinitron API if 
        proxy API is unreachable for fetching spins.
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

# Load environment variables
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE")
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE")
RABBITMQ_ROUTING_KEY = os.getenv("RABBITMQ_ROUTING_KEY")

# Proxy API settings
API_BASE_URL = os.getenv("API_BASE_URL")
SSE_STREAM_URL = f"{API_BASE_URL}/spin-events" if API_BASE_URL else None
PROXY_SPIN_GET_URL = f"{API_BASE_URL}/api/spins" if API_BASE_URL else None

# Primary Spinitron API settings (fallback)
SPINITRON_API_URL = os.getenv("SPINITRON_API_URL")
SPINITRON_API_KEY = os.getenv("SPINITRON_API_KEY")
PRIMARY_SPIN_GET_URL = f"{SPINITRON_API_URL}/spins" if SPINITRON_API_URL else None


logger.debug("Proxy SSE_STREAM_URL: `%s`", SSE_STREAM_URL)
logger.debug("Proxy SPIN_GET_URL: `%s`", PROXY_SPIN_GET_URL)
logger.debug("Primary SPINITRON_API_URL: `%s`", SPINITRON_API_URL)
logger.debug("Primary SPIN_GET_URL: `%s`", PRIMARY_SPIN_GET_URL)


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

# Validate required environment variables
required_proxy_vars = [API_BASE_URL] if not (SPINITRON_API_URL and SPINITRON_API_KEY) else []
required_primary_vars = [SPINITRON_API_URL, SPINITRON_API_KEY] if not API_BASE_URL else []

if not all(
    [
        RABBITMQ_HOST,
        RABBITMQ_USER,
        RABBITMQ_PASS,
        RABBITMQ_QUEUE,
        RABBITMQ_EXCHANGE,
        RABBITMQ_ROUTING_KEY,
    ]
) or not (API_BASE_URL or (SPINITRON_API_URL and SPINITRON_API_KEY)):
    logger.critical(
        "Missing required environment variables. "
        "Ensure RabbitMQ settings are present and either API_BASE_URL (for "
        "proxy) or both SPINITRON_API_URL and SPINITRON_API_KEY (for primary "
        "API) are set."
    )
    sys.exit(1)

if not API_BASE_URL:
    logger.warning(
        "API_BASE_URL for proxy is not set. Will rely solely on primary "
        "Spinitron API if configured."
    )
if not (SPINITRON_API_URL and SPINITRON_API_KEY):
    logger.warning(
        "SPINITRON_API_URL or SPINITRON_API_KEY is not set. Fallback "
        "to primary API will not be available."
    )


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
    Simple helper function to check if the PROXY SSE endpoint is 
    responding.
    """
    if not SSE_STREAM_URL:
        logger.debug(
            "Proxy SSE_STREAM_URL not configured, SSE cannot be reachable."
        )
        return False
    try:
        # Use a shorter timeout for reachability check
        timeout = aiohttp.ClientTimeout(total=5)
        async with HTTP_SESSION.get(SSE_STREAM_URL, timeout=timeout) as response:
            # We just need to connect and get a response, not read the stream
            await response.release() # Release connection immediately
            return response.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.debug("Proxy SSE reachability check failed for %s: %s", SSE_STREAM_URL, e)
        return False


async def fetch_latest_spin():
    """
    Fetch the latest spin.
    
    Tries the proxy API first, then falls back to the primary Spinitron 
    API if configured.
    """
    latest_spin = None

    # Attempt 1: Fetch from Proxy API
    if PROXY_SPIN_GET_URL:
        logger.debug("Attempting to fetch latest spin from proxy: `%s`", PROXY_SPIN_GET_URL)
        try:
            async with HTTP_SESSION.get(PROXY_SPIN_GET_URL, timeout=10) as response:
                if response.status == 200:
                    spins_data = await response.json()
                    items = spins_data.get("items")
                    if items:
                        latest_spin = items[0]
                        logger.info("Successfully fetched latest spin from proxy: `%s - %s`", latest_spin.get("artist"), latest_spin.get("song"))
                        return latest_spin
                    logger.warning("No spins found in proxy response from `%s`.", PROXY_SPIN_GET_URL)
                else:
                    logger.warning(
                        "Failed to fetch spin data from proxy `%s`: Status %s",
                        PROXY_SPIN_GET_URL,
                        response.status,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.warning(
                "Error fetching or parsing spin data from proxy `%s`: %s", PROXY_SPIN_GET_URL, e
            )
    else:
        logger.debug("Proxy SPIN_GET_URL not configured. Skipping proxy attempt.")

    # Attempt 2: Fetch from Primary Spinitron API (if proxy failed or not configured)
    if PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY:
        logger.info("Falling back to primary Spinitron API: `%s`", PRIMARY_SPIN_GET_URL)
        headers = {"Authorization": f"Bearer {SPINITRON_API_KEY}"}
        try:
            async with HTTP_SESSION.get(PRIMARY_SPIN_GET_URL, headers=headers, timeout=10) as response:
                if response.status == 200:
                    spins_data = await response.json()
                    items = spins_data.get("items")
                    if items:
                        latest_spin = items[0]
                        logger.info("Successfully fetched latest spin from primary API: `%s - %s`", latest_spin.get("artist"), latest_spin.get("song"))
                        return latest_spin
                    logger.warning("No spins found in primary API response from `%s`.", PRIMARY_SPIN_GET_URL)
                else:
                    logger.error(
                        "Failed to fetch spin data from primary API `%s`: Status %s",
                        PRIMARY_SPIN_GET_URL,
                        response.status,
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.error(
                "Error fetching or parsing spin data from primary API `%s`: %s", PRIMARY_SPIN_GET_URL, e
            )
    elif not (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
        logger.warning("Primary Spinitron API URL or Key not configured. Cannot fall back.")
    
    if latest_spin is None:
        logger.error("Failed to fetch latest spin from all available sources.")
    return None


async def listen_to_sse(state):
    """
    Connect to the proxy SSE stream and listen for new spin messages.
    When a new spin message is received, fetch the latest spin data 
    (with fallback) and publish it to RabbitMQ. On repeated failures, 
    fall back to polling.
    """
    if not SSE_STREAM_URL:
        logger.warning(
            "Proxy SSE_STREAM_URL is not configured. Cannot listen to SSE. Will proceed to polling "
            "mode if enabled."
        )
        await poll_for_spins(state)
        return

    logger.debug("Listening for SSE at: %s", SSE_STREAM_URL)
    circuit_breaker_failure_count = 0
    retry_count = 0

    while not shutdown_event.is_set():
        if circuit_breaker_failure_count >= CB_ERROR_THRESHOLD:
            logger.error(
                "SSE Circuit breaker triggered. Pausing SSE connection attempts for %d seconds.",
                CB_RESET_TIMEOUT,
            )
            await asyncio.sleep(CB_RESET_TIMEOUT)
            circuit_breaker_failure_count = 0
            retry_count = 0  # Reset retry_count for circuit breaker reset
            continue

        try:
            if retry_count == 0:
                logger.info("Connecting to proxy SSE stream: %s", SSE_STREAM_URL)
            else:
                logger.info("Reconnecting to proxy SSE stream (attempt #%d): %s", retry_count + 1, SSE_STREAM_URL)

            async for event in aiosseclient(SSE_STREAM_URL, session=HTTP_SESSION): # Pass session
                if shutdown_event.is_set():
                    logger.info("Shutting down SSE listener.")
                    break

                if retry_count > 0 or circuit_breaker_failure_count > 0: # Successful connection
                    logger.info("Successfully (re)connected to proxy SSE stream: %s", SSE_STREAM_URL)
                    retry_count = 0
                    circuit_breaker_failure_count = 0 # Also reset CB on successful event

                if event.data == NEW_SPIN_EVENT:
                    logger.debug("Received SSE event: `%s` from proxy", event.data)
                    spin = await fetch_latest_spin() # This now has fallback
                    if spin is not None:
                        await process_spin(spin, state)
                # Reset circuit breaker failure count on any successful event read
                circuit_breaker_failure_count = 0


        except (aiohttp.ClientError, asyncio.TimeoutError, ConnectionRefusedError) as e:
            if shutdown_event.is_set():
                break

            circuit_breaker_failure_count += 1
            retry_count += 1
            logger.warning(
                "Proxy SSE connection failed or dropped (attempt #%d, CB count: %d) for URL %s: %s",
                retry_count,
                circuit_breaker_failure_count,
                SSE_STREAM_URL,
                e,
            )

            if retry_count > MAX_RETRIES:
                logger.warning(
                    "Proxy SSE connection attempts exceeded %d. Switching to polling fallback.",
                    MAX_RETRIES,
                )
                await poll_for_spins(state)
                # If poll_for_spins returns, it means SSE might be available again
                logger.info("Returned from polling mode. Will attempt to reconnect to SSE.")
                retry_count = 0 # Reset retries for SSE
                circuit_breaker_failure_count = 0 # Reset CB for SSE
                continue # Re-enter the SSE connection loop

            backoff = min(2**retry_count + random.uniform(0, 1), 30) # Exponential backoff
            logger.info("Waiting %.2f seconds before next SSE reconnect attempt.", backoff)
            await asyncio.sleep(backoff)
        except Exception as e: # Catch any other unexpected errors from aiosseclient
            if shutdown_event.is_set():
                break
            logger.error("Unexpected error in SSE listener: %s. Restarting SSE connection attempt.", e, exc_info=True)
            circuit_breaker_failure_count += 1 # Count this as a failure for CB
            retry_count += 1
            await asyncio.sleep(5) # Brief pause before retrying


async def poll_for_spins(
    state, poll_interval=POLL_INTERVAL, retry_sse_interval=RETRY_SSE_INTERVAL
):
    """
    Poll for new spins at a fixed interval using fetch_latest_spin (with
    fallback). Periodically check if PROXY SSE is available again, and 
    if so, return to allow the SSE listener to resume.
    """
    if not (PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY)):
        logger.error(
            "Polling impossible: Neither proxy nor primary API for spins is "
            "configured. Shutting down poller."
        )
        shutdown_event.set()
        return

    logger.info(
        "Starting polling mode (interval: %ds). Will check for proxy SSE "
        "availability every %ds.",
        poll_interval,
        retry_sse_interval
    )
    time_of_last_sse_check = time.monotonic()

    while not shutdown_event.is_set():
        try:
            current_spin = await fetch_latest_spin()
            if current_spin is not None:
                await process_spin(current_spin, state)

            # Periodically attempt to see if PROXY SSE is available
            if SSE_STREAM_URL and (time.monotonic() - time_of_last_sse_check >= retry_sse_interval):
                time_of_last_sse_check = time.monotonic()
                logger.debug("Checking if proxy SSE stream is available again...")
                if await sse_is_reachable():
                    logger.info("Proxy SSE stream appears available again. Returning to SSE mode.")
                    return # Exit polling to let listen_to_sse resume

            await asyncio.sleep(poll_interval)

        except (
            aiohttp.ClientError, # Should be caught by fetch_latest_spin, but as a safeguard
            asyncio.TimeoutError,
            aio_pika.exceptions.AMQPError, # For RabbitMQ issues during process_spin
        ) as e:
            logger.error("Error during polling loop: %s", e, exc_info=True)
            # Implement a short backoff for polling errors too, to avoid tight error loops
            await asyncio.sleep(min(poll_interval * 2, 60))
        except Exception as e:  # pylint: disable=broad-except
            logger.critical("Unexpected critical error in polling loop: %s. Stopping.", e, exc_info=True)
            shutdown_event.set() # Critical unhandled error, signal shutdown
            break


async def send_to_rabbitmq(spin_data):
    """
    Publish spin data to RabbitMQ using the persistent connection.
    """
    if RABBITMQ_PUBLISHER is None or RABBITMQ_PUBLISHER.channel is None:
        logger.error("RabbitMQ publisher not available. Cannot send spin data.")
        return
    try:
        await RABBITMQ_PUBLISHER.publish(spin_data)
    except (aio_pika.exceptions.AMQPError, json.JSONDecodeError) as e:
        logger.critical("Error publishing to RabbitMQ: `%s`", e)
    except Exception as e:  # pylint: disable=broad-except
        logger.critical("Unexpected error publishing to RabbitMQ: `%s`", e, exc_info=True)


async def main():
    """
    Entry point for the script.
    Initializes persistent HTTP session and RabbitMQ publisher, then
    starts the SSE listener or polling based on configuration.
    """
    global HTTP_SESSION, RABBITMQ_PUBLISHER  # pylint: disable=global-statement
    logger.info("Starting WBOR Spinitron watchdog...")

    # Initialize persistent HTTP session
    HTTP_SESSION = aiohttp.ClientSession()

    # Initialize RabbitMQ publisher
    if all([RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_EXCHANGE, RABBITMQ_ROUTING_KEY]):
        RABBITMQ_PUBLISHER = RabbitMQPublisher(
            RABBITMQ_HOST,
            RABBITMQ_USER,
            RABBITMQ_PASS,
            RABBITMQ_EXCHANGE,
            RABBITMQ_ROUTING_KEY,
        )
        try:
            await RABBITMQ_PUBLISHER.connect()
            logger.info("Successfully connected to RabbitMQ.")
        except aio_pika.exceptions.AMQPConnectionError as e:
            logger.critical("Failed to connect to RabbitMQ: %s. Watchdog cannot publish messages.", e)
            RABBITMQ_PUBLISHER = None # Ensure it's None if connection failed
    else:
        logger.critical("RabbitMQ configuration is incomplete. Watchdog cannot publish messages.")
        RABBITMQ_PUBLISHER = None

    try:
        # Determine initial mode: SSE if configured, else polling if configured
        if SSE_STREAM_URL:
            logger.info("Primary mode: SSE via proxy.")
            await listen_to_sse(SPIN_STATE)
        elif PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
            logger.info("Primary mode: Polling (SSE proxy not configured).")
            await poll_for_spins(SPIN_STATE)
        else:
            logger.critical(
                "Neither SSE proxy nor any API for fetching spins is "
                "configured. Watchdog cannot operate."
            )
            shutdown_event.set()

        # If listen_to_sse or poll_for_spins exits cleanly due to 
        # shutdown_event, this part is skipped.
        # If they return due to an error that sets shutdown_event, this 
        # loop might not run or run briefly.
        while not shutdown_event.is_set():
            # Keep main alive if a task returns unexpectedly without shutdown
            logger.warning(
                "Main loop entered, a monitoring task may have exited unexpectedly. Re-evaluating."
            )
            if SSE_STREAM_URL and await sse_is_reachable():
                logger.info("Attempting to restart SSE listener.")
                await listen_to_sse(SPIN_STATE)
            elif PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
                logger.info("Attempting to restart Polling.")
                await poll_for_spins(SPIN_STATE)
            else:
                logger.error("No valid operation mode. Setting shutdown.")
                shutdown_event.set()
            await asyncio.sleep(10)


    except ( asyncio.CancelledError):
        logger.info("Main task cancelled.")
    except Exception as e: # Catch-all for unexpected errors in main logic
        logger.critical("Unhandled exception in main: %s", e, exc_info=True)
    finally:
        logger.info("Shutting down connections...")
        if HTTP_SESSION:
            await HTTP_SESSION.close()
            logger.info("HTTP session closed.")
        if RABBITMQ_PUBLISHER and RABBITMQ_PUBLISHER.connection:
            await RABBITMQ_PUBLISHER.close()
            logger.info("RabbitMQ connection closed.")
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown requested via KeyboardInterrupt.")
        # Ensure shutdown_event is set if not already by signal handler
        shutdown_event.set()
    # The finally block in main() should handle cleanup.
    # To ensure graceful exit even if main() itself errors before 
    # setting up signal handlers or if asyncio.run() is interrupted.
    if not shutdown_event.is_set():
        # If shutdown wasn't gracefully triggered from within main

        # This helps in scenarios where KeyboardInterrupt happens very
        # early or if main() exits prematurely without setting the 
        # event. However, tasks might already be finishing or cancelled.
        logger.info("Ensuring shutdown event is set post asyncio.run().")
        shutdown_event.set()