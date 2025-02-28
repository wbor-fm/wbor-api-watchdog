# wbor-api-watchdog

Subscribes to the SSE stream that our [Spinitron](https://spinitron.com) [API proxy](https://github.com/WBOR-91-1-FM/spinitron-proxy/) generates (to indicate that a new Spin was logged). When the message is received, query the latest spin and package it into a message that is sent to a RabbitMQ exchange for downstream consumption (e.g. an [RDS encoder](https://github.com/WBOR-91-1-FM/wbor-rds-encoder)).

If the SSE stream is interrupted, the watchdog will attempt to reconnect 5 times before giving up and switching over to polling. We poll for new spins every 3 seconds while periodically checking for the SSE stream to come back online. Once the SSE stream is back online, we switch back to the SSE stream.

## Usage

Ensure Make is installed on your system.

1. Clone the repository
2. Copy `.env.sample` to `.env` and fill in the values
3. Change values in the Makefile as needed (enter a HOST_DIR)
4. Run `make`
   - Alternatively, run `DOCKER_TOOL=podman make` if you are using Podman
