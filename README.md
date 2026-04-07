# FlexQL

FlexQL is a C++ SQL-style database server with a command-line client and a benchmark runner.

## Requirements

- Linux
- `g++` with C++17 support
- `make`
- `pthread` support

## Build

Build everything from the repository root:

```bash
make
```

This creates:

- `bin/flexql-server`
- `bin/flexql-client`
- `bin/benchmark_flexql_real`
- `./server`
- `./benchmark`

To remove generated server state and built launchers:

```bash
make clean
```

## Project Workflow

The usual workflow is:

1. Build the project.
2. Start the server.
3. Connect with the client or run the benchmark.
4. Stop the server when finished.

### 1. Build

```bash
make
```

### 2. Start the server

Default port:

```bash
./server
```

Explicit port:

```bash
./server 9000
```

Keep the server running in one terminal while using the client or benchmark from another terminal.

### 3. Connect with the client

Open a second terminal and run:

```bash
./bin/flexql-client 127.0.0.1 9000
```

Client usage:

```bash
./bin/flexql-client <host> <port>
```

## Benchmark Workflow

The benchmark runner connects to a FlexQL server at `127.0.0.1:9000`, so make sure the server is already running on that address before starting it.

### Quick benchmark run

Run the helper script:

```bash
./benchmark
```

This uses the default insert target from the source code, which is currently `10000000` rows.

### Benchmark with a custom row count

```bash
./benchmark 100000
./benchmark 1000000
./benchmark 5000000
```

### Unit-test-only mode

Run only the SQL/data validation checks without the large insert benchmark:

```bash
./benchmark --unit-test
```



## What the Benchmark Does

In normal mode, the benchmark:

1. Connects to the server at `127.0.0.1:9000`.
2. Creates a `BIG_USERS` table.
3. Inserts rows in batches of `10000`.
4. Prints progress roughly every 10% of the target row count.
5. Reports elapsed time and throughput in rows per second.
6. Runs additional SQL subset checks and data-level validation queries.

In `--unit-test` mode, it skips the big insert benchmark and only runs the SQL validation checks.


## Notes

- The benchmark currently expects the server on `127.0.0.1:9000`.
- If you start the server on a different port, the benchmark code must be updated or rebuilt to match.
- `make clean` removes generated launch scripts and `data/databases`.
- The benchmark creates tables such as `BIG_USERS`, `TEST_USERS`, and `TEST_ORDERS` during execution.
