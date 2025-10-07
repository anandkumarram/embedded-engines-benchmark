## TiKV Image Batch Benchmark (Java)

This project generates `n` images of size `m x m`, writes them to TiKV using multiple threads, and reads them back to benchmark throughput.

### Prerequisites
- Docker and Docker Compose
- Java 17+
- Maven 3.8+

### Start TiKV locally

```bash
docker compose up -d
# or: docker-compose up -d
```

This brings up PD and 3 TiKV nodes on a single network.

PD endpoint: `127.0.0.1:2379`

### Build

```bash
mvn -q -DskipTests package
```

### Run benchmark

Args: `<numImages> <pixelsPerSide> <imagesDir> <pdAddress> <threads>`

Defaults: `100 256 images 127.0.0.1:2379 <cpu>`

```bash
java -jar target/tikv-benchmark-1.0.0-SNAPSHOT-shaded.jar 500 256 images 127.0.0.1:2379 8
```

The tool will generate images into the directory if not present.

### Modules
- `ImageGenerator`: creates random PNG images.
- `TiKVImageBenchmark`: multi-threaded batch write/read to TiKV RawKV.

### Notes
- Keys are filenames; values are PNG bytes.
- Uses `org.tikv:tikv-client-java` RawKV.

### Monitoring (Prometheus + Grafana)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

Prometheus scrapes:
- PD metrics at `pd:2379/metrics`
- TiKV metrics at `tikv:20180/metrics`

If Grafana has no dashboards, you can import TiKV/TiDB community dashboards from Grafana.com by ID, and point the Prometheus datasource at `http://prometheus:9090` inside the compose network.

## PostgreSQL Benchmark

Start Postgres (included in compose):

```bash
docker compose up -d postgres
```

Build:

```bash
mvn -q -DskipTests package
```

Run Postgres benchmark:

Args: `<numImages> <pixelsPerSide> <imagesDir> <jdbcUrl> <user> <pass> <threads> <batchSize>`

Defaults: `200000 128 images jdbc:postgresql://127.0.0.1:5432/bench bench bench <cpu*2> 10000`

```bash
mvn -q org.codehaus.mojo:exec-maven-plugin:3.3.0:java \
  -Dexec.mainClass=org.example.PostgresImageBenchmark \
  -Dexec.args="200000 128 images jdbc:postgresql://127.0.0.1:5432/bench bench bench 16 10000"
```

## LMDB Benchmark

LMDB runs locally (no Docker required). Data directory defaults to `./lmdbdata`.

```bash
mvn -q org.codehaus.mojo:exec-maven-plugin:3.3.0:java \
  -Dexec.mainClass=org.example.LMDBImageBenchmark \
  -Dexec.args="100000 128 images ./lmdbdata 16 10000"
```

Notes:
- LMDB map size is set to 10GB; adjust in `LMDBImageBenchmark` if needed.
- Writes use a write txn per item within sequential batches; reads use read txns.


