# Go Redis Server Implementation

This repository contains a simple implementation of a Redis server in Go. It is designed to support a variety of basic Redis commands, providing a lightweight alternative to the full Redis server for learning and experimentation purposes.

## Table of Contents

-   [Installation](#installation)
-   [Running the Server](#running-the-server)
-   [Supported Commands](#supported-commands)
    -   [Basic Commands](#basic-commands)
    -   [Stream Commands](#stream-commands)
    -   [Transaction Commands](#transaction-commands)
    -   [Server Configuration Commands](#server-configuration-commands)
-   [Future Work](#future-work)
-   [Contributing](#contributing)
-   [License](#license)

## Installation

To install and run the server, you need to have Go installed on your machine. If you haven't installed Go yet, you can download it from the [official Go website](https://golang.org/dl/).

Clone this repository:

```bash
git clone https://github.com/elordeiro/building-my-own-redis
cd building-my-own-redis
```

## Running the Server

To run the server, execute the following command:

```bash
./spawn_redis_server.sh
```

This script will compile the Go code and start the Redis server.

## Supported Commands

The server currently supports the following Redis commands:

### Basic Commands

-   `PING`: Returns PONG.
-   `ECHO <message>`: Returns the input string.
-   `SET <key> <value>`: Sets a key to a value.
-   `GET <key>`: Gets the value of a key.
-   `INCR <key>`: Increments the integer value of a key.
-   `INFO`: Returns information about the server.
-   `KEYS <pattern>`: Returns all keys matching a pattern.
-   `TYPE <key>`: Returns the type of a key.

### Stream Commands

-   `XADD <stream> <id> <field> <value>`: Adds a message to a stream.
-   `XRANGE <stream> <start> <end>`: Gets a range of messages from a stream.
-   `XREAD STREAMS <stream> <id>`: Reads messages from a stream.
<!-- -   `XREAD COUNT <count> STREAMS <stream> <id>`: Reads messages from a stream. -->

### Transaction Commands

-   `MULTI`: Starts a transaction.
-   `EXEC`: Executes a transaction.
-   `DISCARD`: Discards a transaction.

### Server Configuration Commands

-   `REPLCONF <option> <value>`: Configures replication.
-   `PSYNC <replicaid> <offset>`: Partial synchronization.
-   `WAIT <numreplicas> <timeout>`: Blocks until the specified number of replicas acknowledge the write.
    <!-- -   `CONFIG GET <parameter>`: Gets the value of a configuration parameter. -->
    <!-- -   `CONFIG SET <parameter> <value>`: Sets a configuration parameter. -->

## Future Work

This implementation is a work in progress. Future enhancements may include:

-   Adding support for more Redis commands.
-   Improving performance and concurrency.
-   Adding persistence mechanisms.
-   Enhancing the server's configuration options.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request if you have any improvements or new features to add.

## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.
