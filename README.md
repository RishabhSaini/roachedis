# Run and Test the Project

Steps to build -> run -> test -> stop
```
make init               # Initialize the Go Module and Download Dependencies
make compose            # Build the Images and Start the env using podman compose
make test               # Run the comprehensive Go test client (3 regions)
                        # Test 1: Data written to one region (US-East) is replicated across Redis cache of all regions.
                        # Test 2: Updates made in one region (US-West) are successfully propagated to Redis cache of all regions.
                        # Test 3: Deletions made in one region (EU-West) are reflected in Redis cache across all regions.
make down               # Stop and remove all the running containers
```
Inspect data in Redis and Cockroach
```
./viewDB <SQL Query>    # Execute SQl queries on all DB replicas. Ex: "./viewDB "SELECT * FROM kv_log;"
./viewCache             # View the Redis Cache of all the Regions
```

# Architecture Overview

This project is a geo-distributed key-value store that uses a durable database as the source of truth and regional in-memory caches for fast reads. The system is built on a decoupled, event-driven pattern using Change Data Capture (CDC).

![Single Node Architecture](diagrams/singleNodeArch.png)
![Multi Node Architecture](diagrams/multiNodeArch.png)

## Components

### API Server
A simple Go service that handles client GET, PUT, and DELETE requests. It only writes to the database and reads from the cache.

### CockroachDB
A geo-replicated SQL database that acts as the durable source of truth. All changes are stored as an append-only log.

### Redis Caches
Each region has its own isolated Redis cache for low-latency reads.

### Cache Hydrator
A background service that listens to database changes via CDC and is solely responsible for updating the regional Redis caches.

## How it Works

### Write Path
A write request to any API server is written only to CockroachDB. The database change is then replicated to other regions. The Cache Hydrator service in each region sees this new record via the database's changefeed and updates its local Redis cache accordingly.

### Read Path
A read request first checks the local Redis cache. If the data is present (a cache hit), it's returned immediately. If not (a cache miss), the server reads from its local CockroachDB replica, populates the cache, and then returns the data.
