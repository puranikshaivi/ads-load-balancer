# ADS Lab Assignment

## Build and start
```
docker compose up --build -d
docker compose ps
```

The client connects to `load-balancer:18860` on the Compose network.
The backends are `server1:18861`, `server2:18861`, and `server3:18861`. 
Their host ports `18861`, `18862`, and `18863` are also published for direct testing.

This sends one request through the load balancer:
```
docker compose exec client python /app/client/client.py [word] pride-and-prejudice.txt
```
To test a particular backend directly (for diagnosis only):
```
docker compose exec client python /app/client/client.py [word] pride-and-prejudice.txt --server server1:18861
```
To Watch the demo-style live logs

```
docker compose logs -f load-balancer
```

The individual server logs:

```
docker compose logs -f server1
docker compose logs -f server2
docker compose logs -f server3
```

Or follow all four at once:

```
docker compose logs -f load-balancer server1 server2 server3
```

Each client connection appears as a backend selection in the load-balancer logs. The selected backend name, algorithm, active connection counts, response time estimate, and release are logged. The backend prints its own identifier and whether the Redis cache was hit or missed. Repeated single requests may favor one server when the others are tied; use the concurrent benchmark below to demonstrate distribution more clearly.

## Inspect Redis

Redis server lifecycle and persistence messages:

```
docker compose logs -f redis
```
To watch live Redis commands (including cache `GET`, `SET`, and `INCR` operations), use
a separate terminal:

```
docker compose exec redis redis-cli MONITOR
```

Press `Ctrl+C` to stop `MONITOR`. To inspect stored word-count keys:

```
docker compose exec redis redis-cli KEYS "wordcount:*"
docker compose exec redis redis-cli GET "wordcount:pride-and-prejudice.txt:the"
```

For a clean cache before a benchmark run:

```
docker compose exec redis redis-cli FLUSHDB
```

`FLUSHDB` clears the selected Redis database. Use it only when you are happy to discard the current demo cache.

## Select a load-balancing algorithm

The default algorithm is **Least Connections**. To explicitly start it, run these commands in Command Prompt from the project directory:

```
set "LB_ALGORITHM=LEAST_CONNECTIONS"
docker compose up --build -d --force-recreate load-balancer
set "LB_ALGORITHM="
```

To run **Least Response Time** instead:

```
set "LB_ALGORITHM=LEAST_RESPONSE_TIME"
docker compose up --build -d --force-recreate load-balancer
set "LB_ALGORITHM="
```

Confirm the selected algorithm in the startup log:

```
docker compose logs --tail 20 load-balancer
```

Least Connections chooses the eligible server with the fewest active connections. Least Response Time chooses the server with the lowest exponentially smoothed response-time estimate multiplied by its projected concurrent load (`estimate * (active connections + 1)`). It therefore learns which server has been responding faster, while avoiding sending all new work to a fast server that is already busy. Both algorithms use the same TCP proxy.

## Demonstrate simultaneous requests

The benchmark uses a thread pool to send concurrent requests. Each request opens a fresh RPyC connection, counts one word in the same text file, verifies the returned count against a local count of that file, then closes the connection. This makes each connection a measurable request and allows the load balancer to select a server for it.

With Phase 3 running, use either algorithm and run:

```
docker compose exec client python /app/experiments/benchmark.py --endpoint load-balancer:18860 --requests 300 --concurrency 30 --output /app/results/phase3-least-connections.json
```

The output reports average and p99 client-observed latency, correctness, failures, cache hits, throughput, and the count served by each replica. The summary is also saved in the host `results` directory.

For visible, live output during a run, leave the log terminals open. The benchmark sends up to 30 calls concurrently; the proxy's connection log will show entries similar to:

```text
selected backend=server1:18861 algorithm=LEAST_CONNECTIONS active={...}
selected backend=server3:18861 algorithm=LEAST_CONNECTIONS active={...}
response backend=server1:18861 algorithm=LEAST_CONNECTIONS response_ms=...
```

## Phase 2 baseline (one server, no load balancer)

The one-server service is available through the Compose `phase2` profile. Start only Redis, that server, and the client:

```
docker compose --profile phase2 up --build -d redis phase2-server client
docker compose ps
```

Send a direct Phase 2 request:

```
docker compose exec client python /app/client/client.py the pride-and-prejudice.txt --server phase2-server:18861
```

The same benchmark can target that one server directly:

```
docker compose exec client python /app/experiments/benchmark.py --endpoint phase2-server:18861 --requests 300 --concurrency 30 --output /app/results/phase2-one-server.json
```

Phase 2 publishes the server on host port `18864`. Stop it when finished:

```
docker compose stop phase2-server
```
