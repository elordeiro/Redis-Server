This is a simple implementation of a Redis server in Go.

## Running the server

To run the server, simply run the following command:

```bash
./spawn_redis_server.sh
```

## Server commands

The server supports the following commands:

-   `PING`: Returns PONG
-   `ECHO`: Returns the input string
-   `SET`: Sets a key to a value
-   `GET`: Gets the value of a key
-   `XADD`: Adds a message to a stream
-   `XRANGE`: Gets a range of messages from a stream
-   `XREAD`: Reads messages from a stream
-   `INCR`: Increments a key
-   `INFO`: Returns information about the server
-   `REPLCONF`: Configures replication
-   `PSYNC`: Partial synchronization
-   `WAIT`: Blocks until the specified number of replicas acknowledge the write
-   `KEYS`: Returns all keys matching a pattern
-   `TYPE`: Returns the type of a key
-   `MULTI`: Starts a transaction
-   `EXEC`: Executes a transaction
-   `DISCARD`: Discards a transaction
-   `CONFIG`: Configures the server

... more to come
