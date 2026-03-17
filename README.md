# Kafka Wire Protocol Implementation

A bare-bones implementation of the Kafka Wire Protocol in Java. Built from scratch using raw TCP sockets and byte streams to pass the [CodeCrafters "Build Your Own Kafka" challenge](https://app.codecrafters.io/users/fek247).

## Core Implementation
- **Raw Socket I/O:** Multi-threaded TCP server handling concurrent client connections using `java.net.Socket`.
- **Binary Parsing:** Manual Big-Endian byte decoding for Kafka primitives (`int32`, `int16`, `varint`).
- **Protocol Versioning:** Implemented context-aware parsing for Kafka's Flexible Versions (KIP-482), correctly handling compact arrays, compact strings, and tagged fields without stream corruption.
- **Fault Tolerance:** Robust stream management to prevent deadlocks during abrupt client disconnects (`EOFException`) and missing KRaft metadata files.

## Supported APIs
Handles protocol handshaking, topology discovery, and core messaging pipelines:

| API Key | Name | Supported Versions |
| :--- | :--- | :--- |
| 18 | `API_VERSIONS` | 0 - 4 |
| 75 | `DESCRIBE_TOPIC_PARTITIONS` | 0 |
| 0  | `PRODUCE` | 0 - 11 |
| 1  | `FETCH` | 0 - 17 |

## Usage
Start the broker on port 9092:
```bash
./spawn_kafka.sh