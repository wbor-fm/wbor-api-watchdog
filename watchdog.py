"""
"Watch" for new spins on a Spinitron API proxy and publish them to
RabbitMQ. Listens for SSE updates to indicate a new spin is available,
falling back to polling if SSE becomes unavailable. If the proxy API
for fetching spins is down, it attempts to use the primary Spinitron
API.

Author: Mason Daugherty <@mdrxy>
Version: 1.3.0
Last Modified: 2025-05-16

Changelog:
    - 1.0.0 (2025-03-23): Initial release.
    - 1.1.0 (2025-04-15): Fixes and enhancements to RabbitMQ connection
        management, error handling, and state management.
    - 1.2.0 (2025-05-12): Added fallback to primary Spinitron API if
        proxy API is unreachable for fetching spins. Refactored to
        remove global variables.
    - 1.3.0 (2025-05-16): Added type hinting throughout the script,
        and substantially refactored to ensure robustness and
        maintainability. Improved error handling and logging.
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
import time
import types
from typing import Any, Dict, Optional, Union, cast

import aio_pika
import aio_pika.abc
import aiohttp
import aiormq
from aiosseclient import Event, aiosseclient
from dotenv import load_dotenv

SpinData = Dict[str, Any]

# Assuming utils.logging.configure_logging is correctly defined elsewhere
# For standalone execution, need a simple logging config here.
try:
    from utils.logging import configure_logging

    logging.root.handlers = []
    logger: logging.Logger = configure_logging()
except ImportError:
    # Basic logging if configure_logging is not found
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    logger.warning(
        "`utils.logging.configure_logging` not found! Using basic logging configuration."
    )


load_dotenv()

# RabbitMQ settings
RABBITMQ_HOST: Optional[str] = os.getenv("RABBITMQ_HOST")
RABBITMQ_USER: Optional[str] = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS: Optional[str] = os.getenv("RABBITMQ_PASS")
RABBITMQ_EXCHANGE_NAME: Optional[str] = os.getenv("RABBITMQ_EXCHANGE")
RABBITMQ_ROUTING_KEY_VAL: Optional[str] = os.getenv("RABBITMQ_ROUTING_KEY")

# Proxy API settings
API_BASE_URL: Optional[str] = os.getenv("API_BASE_URL")
SSE_STREAM_URL: Optional[str] = (
    f"{API_BASE_URL.rstrip('/')}/spin-events" if API_BASE_URL else None
)
PROXY_SPIN_GET_URL: Optional[str] = (
    f"{API_BASE_URL.rstrip('/')}/api/spins" if API_BASE_URL else None
)
NEW_SPIN_EVENT_NAME: str = "new spin data"  # Match the Proxy's SSE event
logger.debug("Proxy SSE_STREAM_URL: `%s`", SSE_STREAM_URL)
logger.debug("Proxy SPIN_GET_URL: `%s`", PROXY_SPIN_GET_URL)
logger.debug("Proxy NEW_SPIN_EVENT_NAME: `%s`", NEW_SPIN_EVENT_NAME)

# Primary Spinitron API settings (fallback to proxy)
SPINITRON_API_URL: Optional[str] = os.getenv("SPINITRON_API_URL")
SPINITRON_API_KEY: Optional[str] = os.getenv("SPINITRON_API_KEY")
PRIMARY_SPIN_GET_URL: Optional[str] = (
    f"{SPINITRON_API_URL.rstrip('/')}/spins" if SPINITRON_API_URL else None
)
logger.debug("Primary SPINITRON_API_URL: `%s`", SPINITRON_API_URL)
logger.debug("Primary SPIN_GET_URL: `%s`", PRIMARY_SPIN_GET_URL)

# Configurable retry and circuit breaker parameters
MAX_RETRIES_SSE: int = int(os.getenv("MAX_RETRIES", "5"))
POLL_INTERVAL_CONFIG: int = int(os.getenv("POLL_INTERVAL", "3"))
RETRY_SSE_INTERVAL_CONFIG: int = int(os.getenv("RETRY_SSE_INTERVAL", "300"))
CB_ERROR_THRESHOLD_CONFIG: int = int(os.getenv("CB_ERROR_THRESHOLD", "5"))
CB_RESET_TIMEOUT_CONFIG: int = int(os.getenv("CB_RESET_TIMEOUT", "60"))


class SpinState:  # pylint: disable=too-few-public-methods
    """
    Track the last spin ID to avoid duplicates.
    """

    def __init__(self) -> None:
        # Spin ID can be str or int
        self.last_spin_id: Optional[Union[str, int]] = None


class RabbitMQPublisher:
    """
    Object responsible for managing RabbitMQ connection and message
    publishing.

    Attributes:
    - host (str): RabbitMQ host.
    - user (str): RabbitMQ username.
    - password (str): RabbitMQ password.
    - exchange_name (str): RabbitMQ exchange name.
    - routing_key (str): RabbitMQ routing key.
    - connection (AbstractRobustConnection): RabbitMQ connection.
    - channel (AbstractChannel): RabbitMQ channel.
    - exchange (AbstractExchange): RabbitMQ exchange.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        host: str,
        user: str,
        password: str,
        exchange_name: str,
        routing_key: str,
    ) -> None:
        self.host: str = host
        self.user: str = user
        self.password: str = password
        self.exchange_name: str = exchange_name
        self.routing_key: str = routing_key
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractChannel] = None
        self.exchange: Optional[aio_pika.abc.AbstractExchange] = None

    async def connect(self) -> None:
        """
        Connect to RabbitMQ using the object's credentials and declare
        the exchange (if it doesn't already exist).
        """
        self.connection = await aio_pika.connect_robust(
            host=self.host, login=self.user, password=self.password
        )
        self.channel = await self.connection.channel()
        if self.channel is None:
            logger.error(
                "RabbitMQ channel is None after connection. Cannot declare exchange."
            )
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Channel is None post-connect"
            )

        self.exchange = await self.channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        logger.info("RabbitMQPublisher connected and exchange declared.")

    async def publish(self, spin_data: SpinData) -> None:
        """
        Publish spin data to RabbitMQ.
        Attempts to reconnect if the exchange is not available.

        Parameters:
        - spin_data (SpinData): The spin data to publish.
        """
        message_body: bytes = json.dumps(spin_data).encode("utf-8")
        message: aio_pika.Message = aio_pika.Message(body=message_body)

        if self.exchange is None or (self.channel and self.channel.is_closed):
            logger.warning(
                "Exchange is None or channel closed. Attempting to (re)connect RabbitMQ before "
                "publishing..."
            )
            try:
                await self.connect()
            except Exception as e:
                logger.error(
                    "Failed to (re)connect RabbitMQ during publish attempt: %s", e
                )
                raise aiormq.exceptions.ChannelInvalidStateError(
                    "Reconnect failed during publish"
                ) from e

        if self.exchange is not None:
            # Publish the message to the exchange with the routing key
            await self.exchange.publish(message, routing_key=self.routing_key)
            logger.info(
                "Published spin data to RabbitMQ on `%s` with key `%s`: `%s - %s`",
                self.exchange_name,
                self.routing_key,
                spin_data.get("artist"),
                spin_data.get("song"),
            )
        else:
            logger.error(
                "Failed to publish spin data: exchange is still None after potential reconnect "
                "attempt."
            )
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Exchange is None after reconnect attempt"
            )

    async def close(self) -> None:
        """
        Close the RabbitMQ connection.
        """
        if self.channel and not self.channel.is_closed:
            try:
                await self.channel.close()
                logger.info("RabbitMQ channel closed.")
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("Error closing RabbitMQ channel: %s", e)
        if self.connection and not self.connection.is_closed:
            try:
                await self.connection.close()
                logger.info("RabbitMQ connection closed.")
            except Exception as e:  # pylint: disable=broad-except
                logger.warning("Error closing RabbitMQ connection: %s", e)


class SpinitronWatchdog:
    """
    Monitor the Spinitron API for new spins and publish them to
    RabbitMQ.

    Listens for SSE events and falls back to polling if SSE is
    unavailable. If the proxy API is down, it attempts to use the
    primary Spinitron API.

    Attributes:
    - http_session (ClientSession): HTTP session for API requests.
    - rabbitmq_publisher (RabbitMQPublisher): RabbitMQ publisher for
        sending messages.
    - spin_state (SpinState): State object to track the last spin ID.
    - shutdown_event (Event): Event to signal shutdown.
    """

    def __init__(self) -> None:
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.rabbitmq_publisher: Optional[RabbitMQPublisher] = None
        self.spin_state: SpinState = SpinState()
        self.shutdown_event: asyncio.Event = asyncio.Event()

        if not all(
            [
                RABBITMQ_HOST,
                RABBITMQ_USER,
                RABBITMQ_PASS,
                RABBITMQ_EXCHANGE_NAME,
                RABBITMQ_ROUTING_KEY_VAL,
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

    def _handle_os_signal(
        self, signum_received: int, _frame: Optional[types.FrameType] = None
    ) -> None:
        """
        Handles OS signals like SIGINT and SIGTERM for graceful
        shutdown.

        Parameters:
        - signum_received (int): The signal number received.
        - _frame (Optional[types.FrameType]): The current stack frame.
        """
        try:
            signame: str = signal.Signals(signum_received).name
        except ValueError:
            signame = f"UNKNOWN SIGNAL {signum_received}"
        logger.info("Received OS signal: `%s` (%s)", signame, signum_received)
        self.shutdown_event.set()

    async def _initialize_resources(self) -> None:
        """
        Initializes resources like HTTP session and RabbitMQ publisher.
        """
        logger.info("Initializing resources...")
        self.http_session = aiohttp.ClientSession()

        if all(
            [
                RABBITMQ_HOST,
                RABBITMQ_USER,
                RABBITMQ_PASS,
                RABBITMQ_EXCHANGE_NAME,
                RABBITMQ_ROUTING_KEY_VAL,
            ]
        ):
            # Ensure all required RabbitMQ env vars are strings
            rmq_host = cast(str, RABBITMQ_HOST)
            rmq_user = cast(str, RABBITMQ_USER)
            rmq_pass = cast(str, RABBITMQ_PASS)
            rmq_exchange = cast(str, RABBITMQ_EXCHANGE_NAME)
            rmq_routing_key = cast(str, RABBITMQ_ROUTING_KEY_VAL)

            self.rabbitmq_publisher = RabbitMQPublisher(
                rmq_host,
                rmq_user,
                rmq_pass,
                rmq_exchange,
                rmq_routing_key,
            )
            try:
                await self.rabbitmq_publisher.connect()
                logger.info("Successfully connected to RabbitMQ.")
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.critical(
                    "Failed to connect to RabbitMQ during initialization: `%s`. Watchdog cannot "
                    "publish messages.",
                    e,
                )
                self.rabbitmq_publisher = None
        else:
            logger.critical(
                "RabbitMQ configuration is incomplete. Watchdog cannot publish messages."
            )
            self.rabbitmq_publisher = None

    async def _cleanup_resources(self) -> None:
        """
        Cleans up resources like HTTP session and RabbitMQ publisher.
        """
        logger.info("Shutting down connections...")
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            logger.info("HTTP session closed.")
        if self.rabbitmq_publisher:
            await self.rabbitmq_publisher.close()
        logger.info("Shutdown complete.")

    async def sse_is_reachable(self) -> bool:
        """
        Tests if the SSE stream is reachable.
        """
        if not SSE_STREAM_URL:
            logger.debug(
                "Proxy SSE_STREAM_URL not configured, SSE cannot be reachable."
            )
            return False
        if not self.http_session or self.http_session.closed:
            logger.warning(
                "HTTP session not initialized or closed for SSE reachability check."
            )
            return False
        try:
            timeout: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=5)
            async with self.http_session.get(
                SSE_STREAM_URL, timeout=timeout
            ) as response:
                await response.release()
                return response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(
                "Proxy SSE reachability check failed for `%s`: %s", SSE_STREAM_URL, e
            )
            return False

    async def fetch_latest_spin(  # pylint: disable=too-many-branches
        self,
    ) -> Optional[SpinData]:
        """
        Fetches the latest spin from the proxy API or primary Spinitron
        API.

        Returns:
        - Optional[SpinData]: The latest spin data if successful, else
            None.
        """
        latest_spin: Optional[SpinData] = None
        if not self.http_session or self.http_session.closed:
            logger.error("HTTP session not initialized or closed for fetching spin.")
            return None

        # Attempt 1: Fetch from Proxy API
        if PROXY_SPIN_GET_URL:
            logger.debug(
                "Attempting to fetch latest spin from proxy: `%s`", PROXY_SPIN_GET_URL
            )
            try:
                async with self.http_session.get(
                    PROXY_SPIN_GET_URL, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        spins_data_raw: Any = await response.json()
                        if (
                            isinstance(spins_data_raw, dict)
                            and "items" in spins_data_raw
                            and isinstance(spins_data_raw["items"], list)
                            and spins_data_raw["items"]
                        ):
                            latest_spin = cast(SpinData, spins_data_raw["items"][0])
                            logger.info(
                                "Successfully fetched latest spin from proxy: `%s - %s`",
                                latest_spin.get("artist"),
                                latest_spin.get("song"),
                            )
                            return latest_spin
                        logger.warning(
                            "No spins or unexpected format in proxy response from `%s`.",
                            PROXY_SPIN_GET_URL,
                        )
                    else:
                        logger.warning(
                            "Failed to fetch spin data from proxy `%s`: Status %s",
                            PROXY_SPIN_GET_URL,
                            response.status,
                        )
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
            ) as e:
                logger.warning(
                    "Error fetching or parsing spin data from proxy `%s`: %s",
                    PROXY_SPIN_GET_URL,
                    e,
                )
        else:
            logger.debug("Proxy SPIN_GET_URL not configured. Skipping proxy attempt.")

        # Attempt 2: Proxy did not return, so attempt to fetch from Primary Spinitron API
        if latest_spin is None and PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY:
            logger.info(
                "Falling back to primary Spinitron API: `%s`", PRIMARY_SPIN_GET_URL
            )
            headers: Dict[str, str] = {"Authorization": f"Bearer {SPINITRON_API_KEY}"}
            try:
                async with self.http_session.get(
                    PRIMARY_SPIN_GET_URL,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as response:
                    if response.status == 200:
                        spins_data_raw = await response.json()
                        if (
                            isinstance(spins_data_raw, dict)
                            and "items" in spins_data_raw
                            and isinstance(spins_data_raw["items"], list)
                            and spins_data_raw["items"]
                        ):
                            latest_spin = cast(SpinData, spins_data_raw["items"][0])
                            logger.info(
                                "Successfully fetched latest spin from primary API: `%s - %s`",
                                latest_spin.get("artist"),
                                latest_spin.get("song"),
                            )
                            return latest_spin
                        logger.warning(
                            "No spins or unexpected format in primary API response from `%s`.",
                            PRIMARY_SPIN_GET_URL,
                        )
                    else:
                        logger.error(
                            "Failed to fetch spin data from primary API `%s`: Status %s",
                            PRIMARY_SPIN_GET_URL,
                            response.status,
                        )
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
            ) as e:
                logger.error(
                    "Error fetching or parsing spin data from primary API `%s`: %s",
                    PRIMARY_SPIN_GET_URL,
                    e,
                )
        elif latest_spin is None and not (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
            logger.warning(
                "Primary Spinitron API URL or Key not configured. Cannot fall back."
            )

        if latest_spin is None:
            logger.error("Failed to fetch latest spin from all available sources.")
        return latest_spin

    async def send_to_rabbitmq(self, spin_data: SpinData) -> None:
        """
        Sends a single spin's data to RabbitMQ (with retry logic).

        Parameters:
        - spin_data (SpinData): Data to send.
        """
        if self.rabbitmq_publisher is None:
            logger.error(
                "RabbitMQ publisher is not initialized. Cannot send spin data for: `%s - %s`",
                spin_data.get("artist"),
                spin_data.get("song"),
            )
            return

        # Check and attempt reconnect if channel is problematic
        if (
            self.rabbitmq_publisher.channel is None
            or (
                self.rabbitmq_publisher.channel
                and self.rabbitmq_publisher.channel.is_closed
            )
            or (
                self.rabbitmq_publisher.connection
                and self.rabbitmq_publisher.connection.is_closed
            )
        ):
            logger.warning(
                "RabbitMQ publisher's channel/connection is None or closed. Attempting to "
                "(re)connect."
            )
            try:
                await self.rabbitmq_publisher.connect()
                logger.info(
                    "RabbitMQ publisher (re)connected successfully via send_to_rabbitmq()."
                )
            except Exception as e:  # pylint: disable=broad-except
                logger.error(
                    "Failed to (re)connect RabbitMQ publisher in send_to_rabbitmq(): `%s`. Spin "
                    "data for `%s - %s` may be lost if retries also fail.",
                    e,
                    spin_data.get("artist"),
                    spin_data.get("song"),
                )

        max_publish_retries: int = int(os.getenv("RABBITMQ_PUBLISH_RETRIES", "3"))
        publish_retry_delay: int = int(os.getenv("RABBITMQ_PUBLISH_RETRY_DELAY", "5"))

        for attempt in range(max_publish_retries):
            try:
                # Ensure publisher is still valid before attempting a publish
                if (
                    self.rabbitmq_publisher is None
                ):  # Could be set to None if the initial connection failed badly
                    logger.error(
                        "RabbitMQ publisher became None before publish attempt %d. Message lost.",
                        attempt + 1,
                    )
                    return
                await self.rabbitmq_publisher.publish(spin_data)
                return  # Successly published, exit the loop
            except aiormq.exceptions.ChannelInvalidStateError as e:
                logger.warning(
                    "Publish attempt %d/%d failed: Channel is closed. Spin: `%s - %s`. Error: %s",
                    attempt + 1,
                    max_publish_retries,
                    spin_data.get("artist"),
                    spin_data.get("song"),
                    e,
                )
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.warning(
                    "Publish attempt %d/%d failed: AMQP Connection Error. Spin: `%s - %s`. "
                    "Error: %s",
                    attempt + 1,
                    max_publish_retries,
                    spin_data.get("artist"),
                    spin_data.get("song"),
                    e,
                )
            except (
                Exception  # pylint: disable=broad-except
            ) as e:  # Catch any other unexpected error during the publish call
                logger.critical(
                    "BREAKING: unexpected error during publish attempt %d/%d for spin `%s - %s`: "
                    "%s",
                    attempt + 1,
                    max_publish_retries,
                    spin_data.get("artist"),
                    spin_data.get("song"),
                    e,
                    exc_info=True,
                )
                break  # For truly unexpected errors, break retry loop

            # If not the last attempt, wait and retry
            if attempt < max_publish_retries - 1:
                logger.info("Retrying publish in %d seconds...", publish_retry_delay)
                await asyncio.sleep(publish_retry_delay)
            else:  # Last attempt failed
                logger.critical(
                    "All %d publish attempts failed for spin `%s - %s`. Message lost.",
                    max_publish_retries,
                    spin_data.get("artist"),
                    spin_data.get("song"),
                )

    async def process_spin(self, spin: SpinData) -> None:
        """
        Processes a new spin: checks for duplicates and publishes to the
        queue if it's a new spin.

        Parameters:
        - spin (SpinData): Data to process.
        """
        spin_id: Optional[Union[str, int]] = spin.get("id")

        # Basic comparison of spin IDs
        if spin_id != self.spin_state.last_spin_id:
            self.spin_state.last_spin_id = spin_id
            await self.send_to_rabbitmq(spin)
        else:
            logger.info(
                "Duplicate spin received (ID: `%s`). Skipping publish.", spin_id
            )

    async def listen_to_sse(  # pylint: disable=too-many-branches, too-many-statements
        self,
    ) -> None:
        """
        Loop to listen for SSE events and process new spins.
        """
        if not SSE_STREAM_URL:
            logger.warning(
                "Proxy SSE_STREAM_URL is not configured. Switching to polling."
            )
            await self.poll_for_spins()
            return

        logger.debug("Listening for SSE at: `%s`", SSE_STREAM_URL)
        circuit_breaker_failure_count: int = 0
        retry_count: int = 0

        while not self.shutdown_event.is_set():
            if circuit_breaker_failure_count >= CB_ERROR_THRESHOLD_CONFIG:
                logger.error(
                    "SSE Circuit breaker triggered. Pausing SSE attempts for %d seconds.",
                    CB_RESET_TIMEOUT_CONFIG,
                )
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(), timeout=CB_RESET_TIMEOUT_CONFIG
                    )
                    if self.shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    # Timeout expired, continue to next iteration
                    pass
                circuit_breaker_failure_count = 0
                retry_count = 0
                continue

            try:
                if retry_count == 0:
                    logger.info("Connecting to proxy SSE stream: `%s`", SSE_STREAM_URL)
                else:
                    logger.info(
                        "Reconnecting to proxy SSE stream (attempt #%d): `%s`",
                        retry_count + 1,
                        SSE_STREAM_URL,
                    )

                # Ensure `http_session` is available for `aiosseclient`
                if not self.http_session or self.http_session.closed:
                    logger.warning(
                        "HTTP session for SSE is closed or None. Re-initializing."
                    )
                    if self.http_session and not self.http_session.closed:
                        await self.http_session.close()
                    self.http_session = aiohttp.ClientSession()

                event_item: Event
                async for event_item in aiosseclient(SSE_STREAM_URL):
                    if self.shutdown_event.is_set():
                        logger.info("Shutting down SSE listener.")
                        break
                    if retry_count > 0 or circuit_breaker_failure_count > 0:
                        logger.info(
                            "Successfully (re)connected to proxy SSE stream: `%s`",
                            SSE_STREAM_URL,
                        )
                        retry_count = 0
                        circuit_breaker_failure_count = 0
                    if event_item.data == NEW_SPIN_EVENT_NAME:
                        logger.debug(
                            "Received SSE event: `%s` from proxy", event_item.data
                        )
                        spin: Optional[SpinData] = await self.fetch_latest_spin()
                        if spin:
                            await self.process_spin(spin)
                    circuit_breaker_failure_count = 0  # Reset on any successful event
                if self.shutdown_event.is_set():
                    break

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                ConnectionRefusedError,
            ) as e:
                if self.shutdown_event.is_set():
                    break
                circuit_breaker_failure_count += 1
                retry_count += 1
                logger.warning(
                    "Proxy SSE connection error (attempt #%d, CB count: %d) for `%s`: %s",
                    retry_count,
                    circuit_breaker_failure_count,
                    SSE_STREAM_URL,
                    e,
                )
                if retry_count > MAX_RETRIES_SSE:
                    logger.warning("Max SSE retries exceeded. Switching to polling.")
                    await self.poll_for_spins()
                    logger.info(
                        "Returned from polling (detected SSE reconnect). Resetting SSE "
                        "retry counters."
                    )
                    retry_count = 0
                    circuit_breaker_failure_count = 0
                    continue
                backoff: float = min(2**retry_count + random.uniform(0, 1), 30)
                logger.info("Waiting %.2fs before next SSE reconnect.", backoff)
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=backoff)
                    if self.shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass
            except Exception as e:  # pylint: disable=broad-except
                if self.shutdown_event.is_set():
                    break
                logger.error(
                    "Unexpected error in SSE listener: %s. Restarting SSE attempt.",
                    e,
                    exc_info=True,
                )
                circuit_breaker_failure_count += 1
                retry_count += 1
                try:
                    await asyncio.wait_for(self.shutdown_event.wait(), timeout=5)
                    if self.shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass

    async def poll_for_spins(
        self,
        poll_interval: Optional[int] = None,
        retry_sse_interval: Optional[int] = None,
    ) -> None:
        """
        Polls for new spins if SSE is unavailable or not configured.
        Continues to check for SSE availability at regular intervals.
        If SSE becomes available, it exits the polling loop.

        Parameters:
        - poll_interval (Optional[int]): The interval to poll for spins.
        - retry_sse_interval (Optional[int]): The interval to check for SSE availability.
        """
        current_poll_interval: int = (
            poll_interval if poll_interval is not None else POLL_INTERVAL_CONFIG
        )
        current_retry_sse_interval: int = (
            retry_sse_interval
            if retry_sse_interval is not None
            else RETRY_SSE_INTERVAL_CONFIG
        )

        if not (PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY)):
            logger.error(
                "Polling impossible: No spin fetch URLs configured. Shutting down."
            )
            self.shutdown_event.set()
            return

        logger.info(
            "Starting polling mode (interval: %ds). SSE check every %ds.",
            current_poll_interval,
            current_retry_sse_interval,
        )
        time_of_last_sse_check: float = time.monotonic()

        while not self.shutdown_event.is_set():
            try:
                current_spin: Optional[SpinData] = await self.fetch_latest_spin()
                if current_spin:
                    await self.process_spin(current_spin)

                if SSE_STREAM_URL and (
                    time.monotonic() - time_of_last_sse_check
                    >= current_retry_sse_interval
                ):
                    time_of_last_sse_check = time.monotonic()
                    logger.debug(
                        "Polling: Checking if proxy SSE stream is available again..."
                    )
                    if await self.sse_is_reachable():
                        logger.info(
                            "Polling: Proxy SSE stream available. Returning to SSE mode."
                        )
                        return  # Exit polling to resume SSE

                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(), timeout=current_poll_interval
                    )
                    if self.shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass  # Polling interval elapsed

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                aio_pika.exceptions.AMQPError,
            ) as e:
                logger.error("Error during polling loop: %s", e, exc_info=True)
                backoff_poll: int = min(current_poll_interval * 2, 60)
                logger.info("Polling error, backing off for %d seconds.", backoff_poll)
                try:
                    await asyncio.wait_for(
                        self.shutdown_event.wait(), timeout=backoff_poll
                    )
                    if self.shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass
            except Exception as e:  # pylint: disable=broad-except
                logger.critical(
                    "STOPPING: Unexpected critical error in polling loop: %s",
                    e,
                    exc_info=True,
                )
                self.shutdown_event.set()
                break

    async def run(self) -> None:  # pylint: disable=too-many-branches
        """
        Main entry point to run the watchdog. Sets up signal handlers,
        initializes resources, and starts the main loop for either SSE
        or polling.
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        for sig_name_str in ("SIGINT", "SIGTERM"):
            sig_val: Optional[signal.Signals] = getattr(signal, sig_name_str, None)
            if sig_val is not None:
                loop.add_signal_handler(sig_val, self._handle_os_signal, sig_val)

        await self._initialize_resources()

        try:
            if SSE_STREAM_URL:
                logger.info("Primary mode: SSE via proxy.")
                await self.listen_to_sse()
            elif PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
                logger.info("Primary mode: Polling (SSE proxy not configured).")
                await self.poll_for_spins()
            else:
                logger.critical(
                    "Neither SSE nor any spin fetch API configured. Watchdog cannot operate."
                )
                self.shutdown_event.set()

            # Fallback loop if primary tasks exit without setting shutdown
            # This should ideally not be reached if tasks are robust!
            while not self.shutdown_event.is_set():
                logger.warning(
                    "Main monitoring task exited unexpectedly. Re-evaluating mode in 10s..."
                )
                await asyncio.sleep(10)
                if self.shutdown_event.is_set():
                    break

                if SSE_STREAM_URL and await self.sse_is_reachable():
                    logger.info(
                        "Attempting to restart SSE listener after unexpected exit."
                    )
                    await self.listen_to_sse()
                elif PROXY_SPIN_GET_URL or (PRIMARY_SPIN_GET_URL and SPINITRON_API_KEY):
                    logger.info("Attempting to restart Polling after unexpected exit.")
                    await self.poll_for_spins()
                else:
                    logger.error(
                        "No valid operation mode after unexpected exit. Setting shutdown..."
                    )
                    self.shutdown_event.set()

        except asyncio.CancelledError:
            logger.info("Main run task cancelled.")
        except Exception as e:  # pylint: disable=broad-except
            logger.critical(
                "Unhandled exception in SpinitronWatchdog run: %s", e, exc_info=True
            )
            self.shutdown_event.set()
        finally:
            await self._cleanup_resources()


if __name__ == "__main__":
    """
    Initializes and runs the SpinitronWatchdog.
    """

    watchdog = SpinitronWatchdog()
    try:
        asyncio.run(watchdog.run())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received in __main__. Requesting shutdown.")
        if not watchdog.shutdown_event.is_set():
            watchdog.shutdown_event.set()
    except (
        Exception  # pylint: disable=broad-except
    ) as e:  # Catch-all for errors during asyncio.run or very early instantiation
        logger.critical("Critical error in __main__ execution: %s", e, exc_info=True)
        if (
            watchdog and not watchdog.shutdown_event.is_set()
        ):  # Ensure shutdown if watchdog exists
            watchdog.shutdown_event.set()
    finally:
        # Final check, though watchdog.run() should handle its own cleanup.
        if watchdog and not watchdog.shutdown_event.is_set():
            logger.info(
                "Application exiting, ensuring shutdown event is set from __main__."
            )
            watchdog.shutdown_event.set()
        logger.info("Application exiting __main__ block.")
