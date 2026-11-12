# StarRocks API - Backend

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance Go API gateway for StarRocks, providing multi-environment query execution, streaming exports, schema introspection, routine load management, batch load & stream load, query monitoring, and role-based access control.

This is the backend. The matching web interface is [starrocks-admin-ui](https://github.com/PttemDev/starrocks-admin-ui).

## Quick Start

Requires Go 1.25 or newer (use the latest patch release).

```bash
cp config.example.json config.json   # edit with your StarRocks hosts, credentials and tokens
go build -o bin/server .
./bin/server -config config.json
```

The server starts on `127.0.0.1:8081` by default. Override via `config.json`.

`config.json` holds database passwords and API tokens. It is gitignored. Never commit it, and replace every placeholder value (`your-*-token-here`, `change-me`) before you expose the server.

Quick check: `curl http://127.0.0.1:8081/api/health` returns `{"status":"ok"}`, and `/api/docs` serves the interactive API reference.

## Docker

No configuration is baked into the image. Build it and mount your own config at runtime:

```bash
docker build -t starrocks-api .

docker run -d -p 8081:8081 --name starrocks-api \
  -v $(pwd)/config.json:/etc/starrocks-api/config.json:ro \
  starrocks-api
```

If your containers cannot reach the host network, set `server.host` to `0.0.0.0` in the config so the published port works.

### Read-only vs read/write environments

Each environment in the config has a `read_only` flag. A common setup is to run two deployments with the same list of environments and flip `read_only` per deployment: you can browse every cluster from either deployment, but only write to the one that deployment is meant to manage.

---

## Configuration

### Single FE Node

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 8081,
    "read_timeout_seconds": 30,
    "write_timeout_seconds": 300,
    "cors_origins": ["http://localhost:5173"]
  },
  "environments": [
    {
      "name": "prod",
      "display_name": "Production",
      "host": "192.168.1.10",
      "port": 9030,
      "user": "root",
      "password": "",
      "database": "",
      "max_open_conns": 50,
      "max_idle_conns": 10,
      "conn_max_lifetime_seconds": 300,
      "read_only": true
    }
  ],
  "auth_tokens": [
    { "tenant": "admin-ui", "token": "your-admin-token-here", "role": "admin" },
    { "tenant": "viewer-ui", "token": "your-viewer-token-here", "role": "viewer" }
  ],
  "login_users": [
    { "username": "admin", "password": "change-me", "role": "admin" },
    { "username": "viewer", "password": "change-me", "role": "viewer" }
  ]
}
```

### Multi-FE Failover (recommended for production)

Use the `hosts` array instead of `host`+`port`. The driver tries each address in order and automatically fails over on connection failure. No application-level retry needed.

```json
{
  "environments": [
    {
      "name": "prod",
      "display_name": "Production",
      "hosts": [
        "fe1.starrocks.internal:9030",
        "fe2.starrocks.internal:9030",
        "fe3.starrocks.internal:9030"
      ],
      "user": "root",
      "password": "",
      "database": "",
      "max_open_conns": 50,
      "max_idle_conns": 10,
      "conn_max_lifetime_seconds": 300,
      "read_only": true,
      "default_replication_num": 2,
      "fe_http_port": 8030
    }
  ]
}
```

**How failover works:** The underlying MySQL driver (`go-sql-driver/mysql`) builds a DSN like `user:pass@tcp(fe1:9030,fe2:9030,fe3:9030)/db`. When a connection is opened or an existing connection fails, the driver rotates through the address list. Combined with `conn_max_lifetime_seconds`, connections are recycled periodically, naturally distributing load across FE nodes.

### Config Reference

| Field | Type | Default | Description |
|---|---|---|---|
| **server.host** | string | `127.0.0.1` | Bind address |
| **server.port** | int | `8080` | Listen port |
| **server.read_timeout_seconds** | int | `30` | HTTP read timeout |
| **server.write_timeout_seconds** | int | `300` | HTTP write timeout (set high for streaming) |
| **server.cors_origins** | string[] | `[]` | Allowed CORS origins. Supports wildcard suffixes (e.g. `*.example.com`) |
| **environments[].name** | string | required | Unique environment identifier |
| **environments[].display_name** | string | `name` | Human-readable name shown in UI |
| **environments[].host** | string | - | Single FE host (use this OR `hosts`) |
| **environments[].port** | int | - | FE MySQL port (required with `host`) |
| **environments[].hosts** | string[] | - | Multi-FE `host:port` list for failover (use this OR `host`+`port`) |
| **environments[].user** | string | required | StarRocks user |
| **environments[].password** | string | `""` | StarRocks password |
| **environments[].database** | string | `""` | Default database (empty = no default) |
| **environments[].max_open_conns** | int | `25` | Max open connections in pool |
| **environments[].max_idle_conns** | int | `5` | Max idle connections in pool |
| **environments[].conn_max_lifetime_seconds** | int | `300` | Max connection lifetime before recycle |
| **environments[].read_only** | bool | `false` | Block DDL/DML statements (SELECT/SHOW only) |
| **environments[].default_replication_num** | int | `3` | Default replication number for CREATE TABLE |
| **environments[].fe_http_port** | int | `8030` | FE HTTP port (used for Stream Load) |
| **auth_tokens[].tenant** | string | required | Identifier for the token holder |
| **auth_tokens[].token** | string | required | Bearer token value |
| **auth_tokens[].role** | string | `admin` | Role: `admin` or `viewer` |
| **login_users[].username** | string | required | Login username |
| **login_users[].password** | string | required | Login password |
| **login_users[].role** | string | `admin` | Role: `admin` or `viewer` |

---

## Authentication & Authorization

### Authentication

All API endpoints (except `/api/health`, `/api/ready`, `/api/docs*`, and `/api/login`) require a Bearer token via the `Authorization` header:

```
Authorization: Bearer your-admin-token-here
```

Tokens are validated against the `auth_tokens` list in the config file. Each token maps to a tenant name and a role.

Users can obtain a token by logging in via `POST /api/login` with username and password. The server returns a token matching the user's configured role.

### Role-Based Access Control (RBAC)

There are two roles:

| Role | Description |
|---|---|
| **admin** | Full access. Can execute write queries, create/drop tables, manage routine loads, manage batch loads, stream load file uploads, kill queries. |
| **viewer** | Read-only access. Can browse schema, execute SELECT/SHOW queries, view monitoring data, view batch load jobs. Cannot modify data or schema. |

The role is determined by the auth token used. Admin-only endpoints return HTTP `403` for viewer tokens.

Additionally, environments marked `read_only: true` block all DDL/DML statements regardless of role.

---

## API Reference

All environment-scoped endpoints require the `X-Environment` header with the environment name. Responses are JSON (`application/json`) unless otherwise noted.

### Common Headers

```
Authorization: Bearer <token>     # required for all except /health and /login
X-Environment: prod               # required for environment-scoped endpoints
Content-Type: application/json    # required for POST requests (except stream-load)
```

### Error Format

All errors return:

```json
{
  "error": "human-readable error message"
}
```

HTTP status codes: `400` bad request, `401` unauthorized, `403` forbidden (read-only / admin-only), `422` SQL error, `500` server error.

---

## Public Endpoints

### `GET /api/health`

Liveness probe. Returns 200 if the process is running. No authentication or environment header needed.

**Response (200):**
```json
{"status": "ok"}
```

---

### `GET /api/ready`

Readiness probe. Pings every configured environment's database connection pool and reports per-environment status. Returns 200 when all environments are reachable, 503 otherwise. No authentication or environment header needed.

**Response (200 — all environments reachable):**
```json
{
  "status": "ready",
  "uptime": "2h35m12s",
  "environments": [
    {"name": "prod", "ready": true, "latency_ms": 3},
    {"name": "test", "ready": true, "latency_ms": 5}
  ]
}
```

**Response (503 — one or more environments unreachable):**
```json
{
  "status": "not_ready",
  "uptime": "2h35m12s",
  "environments": [
    {"name": "prod", "ready": true, "latency_ms": 3},
    {"name": "test", "ready": false, "latency_ms": 5000, "error": "dial tcp: connection refused"}
  ]
}
```

---

### `GET /api/docs`

Interactive API reference powered by [Scalar](https://scalar.com). Browse and try all endpoints from the browser.

### `GET /api/docs/openapi.yaml`

Raw OpenAPI 3.0 specification in YAML format.

---

### `POST /api/login`

Authenticate with username and password. Returns a Bearer token and role.

**Request:**
```json
{
  "username": "admin",
  "password": "change-me"
}
```

**Response (200):**
```json
{
  "success": true,
  "token": "your-admin-token-here",
  "role": "admin"
}
```

---

## Authenticated Endpoints

All endpoints below require `Authorization: Bearer <token>`.

### `GET /api/environments`

List all configured environments. No environment header needed. Sensitive fields (host, password) are excluded.

**Response:**
```json
[
  {
    "name": "prod",
    "display_name": "Production",
    "read_only": true,
    "default_replication_num": 2
  },
  {
    "name": "test",
    "display_name": "Test",
    "read_only": false,
    "default_replication_num": 3
  }
]
```

---

## Environment-Scoped Endpoints (Viewer + Admin)

All endpoints below additionally require `X-Environment` header.

### `POST /api/query`

Execute a SQL query. Supports two response formats.

**Request:**
```json
{
  "sql": "SELECT * FROM db.table LIMIT 100",
  "format": "json",
  "session_variables": {
    "enable_topn_opt": "false"
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `sql` | string | required | SQL statement to execute |
| `format` | string | `ndjson` | Response format: `ndjson` or `json` |
| `session_variables` | object | `{}` | Key-value pairs executed as `SET key = 'value'` on the connection before the query. Useful for optimizer hints like `enable_topn_opt`, `query_timeout`, etc. |

**Read-only enforcement:** On read-only environments, DDL/DML statements are blocked with HTTP 403.

#### Format: `json` (recommended for B2B integrations)

Returns a single JSON object. Easy to consume with any HTTP client.

**Response:** `Content-Type: application/json`
```json
{
  "columns": ["id", "name", "created_at"],
  "types": ["BIGINT", "VARCHAR", "DATETIME"],
  "rows": [
    {"id": 1, "name": "Alice", "created_at": "2025-01-15 10:30:00"},
    {"id": 2, "name": "Bob", "created_at": "2025-01-16 11:00:00"}
  ]
}
```

**curl:**
```bash
curl -s -X POST http://localhost:8081/api/query \
  -H "Authorization: Bearer your-admin-token-here" \
  -H "X-Environment: prod" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SHOW DATABASES", "format": "json"}' | jq .
```

**Python:**
```python
import requests

resp = requests.post(
    "http://localhost:8081/api/query",
    headers={
        "Authorization": "Bearer your-admin-token-here",
        "X-Environment": "prod",
    },
    json={"sql": "SELECT * FROM mydb.events LIMIT 1000", "format": "json"},
)
data = resp.json()
print(data["columns"])  # ["id", "name", "created_at"]
for row in data["rows"]:
    print(row)
```

#### Format: `ndjson` (default, best for large result sets)

Streams rows as newline-delimited JSON. O(1) memory regardless of result size.

**Response:** `Content-Type: application/x-ndjson`

**Line 1 - Metadata:**
```json
{"type":"metadata","columns":["id","name","created_at"],"types":["BIGINT","VARCHAR","DATETIME"]}
```

**Lines 2..N - Data rows:**
```json
{"id":1,"name":"Alice","created_at":"2025-01-15 10:30:00"}
{"id":2,"name":"Bob","created_at":"2025-01-16 11:00:00"}
```

**Python (streaming):**
```python
import requests, json

resp = requests.post(
    "http://localhost:8081/api/query",
    headers={
        "Authorization": "Bearer your-admin-token-here",
        "X-Environment": "prod",
    },
    json={"sql": "SELECT * FROM mydb.events LIMIT 1000"},
    stream=True
)

for line in resp.iter_lines(decode_unicode=True):
    if not line.strip():
        continue
    obj = json.loads(line)
    if obj.get("type") == "metadata":
        print(f"Columns: {obj['columns']}")
    elif obj.get("type") == "error":
        print(f"Error: {obj['error']}")
    else:
        print(obj)  # data row
```

---

### `POST /api/export`

Export query results as a downloadable file.

**Request:**
```json
{
  "sql": "SELECT * FROM mydb.events WHERE date = '2025-01-15'",
  "format": "csv",
  "session_variables": {
    "enable_topn_opt": "false"
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `sql` | string | required | SQL statement to execute |
| `format` | string | `csv` | Export format: `csv`, `excel`, or `json` |
| `session_variables` | object | `{}` | Key-value pairs executed as `SET key = 'value'` on the connection before the query |

**Response:** Binary file download with `Content-Disposition` header.

---

### `GET /api/nodes`

Returns StarRocks cluster node status (backends + frontends).

---

### `GET /api/routines`

Returns all routine load jobs (all states: RUNNING, PAUSED, STOPPED, CANCELLED) across all user databases. Iterates each database and runs `SHOW ALL ROUTINE LOAD FROM <db>`. System databases (`_statistics_`, `information_schema`, `sys`) are excluded.

---

### `GET /api/ingestion?db=<database>`

Returns routine load jobs for a specific database.

---

### `GET /api/databases`

List all database names.

---

### `GET /api/tables?db=<database>`

List all table names in a database.

---

### `GET /api/columns?db=<database>&table=<table>`

Get column metadata for a specific table.

---

### `GET /api/table-details?db=<database>`

Detailed table information (row counts, data sizes, timestamps). Correctly distinguishes between regular views and materialized views by cross-referencing `information_schema.materialized_views`.

Type values returned: `BASE TABLE`, `VIEW`, `MATERIALIZED VIEW`.

---

### `GET /api/show-create?db=<database>&table=<name>&type=<type>`

Returns the creation DDL for a table, view, or materialized view.

| Parameter | Required | Description |
|---|---|---|
| `db` | yes | Database name |
| `table` | yes | Table/view/MV name |
| `type` | yes | One of: `BASE TABLE`, `VIEW`, `MATERIALIZED VIEW` |

Uses `SHOW CREATE TABLE`, `SHOW CREATE VIEW`, or `SHOW CREATE MATERIALIZED VIEW` based on the `type` parameter.

**Response:**
```json
{
  "name": "mv_order_summary",
  "type": "MATERIALIZED VIEW",
  "ddl": "CREATE MATERIALIZED VIEW ..."
}
```

---

### `GET /api/routine-load/list?db=<database>&table=<table>`

List routine load jobs (all states) for a specific database using `SHOW ALL ROUTINE LOAD FROM <db>`. Optionally filtered by table name (client-side).

---

### `GET /api/batch-load/list?db=<database>&limit=<n>`

List recent batch load jobs for a database. Tries `information_schema.loads` first (StarRocks 3.x — includes Stream Load, Broker Load, INSERT), falls back to `SHOW LOAD`.

When `db` is omitted, returns jobs across all user databases (system databases like `_statistics_` are excluded).

| Parameter | Required | Default | Description |
|---|---|---|---|
| `db` | no | - | Database name. Omit for all databases |
| `limit` | no | `50` | Maximum number of jobs to return |

---

### Query Monitoring

| Endpoint | Description |
|---|---|
| `GET /api/process-list` | Running processes (`SHOW PROCESSLIST`) |
| `GET /api/current-queries` | Active queries with detail (`SHOW PROC "/current_queries"`) |
| `GET /api/slow-queries?limit=100` | Slow query log |

---

## Admin-Only Endpoints

These endpoints require an admin-role token. Viewer tokens receive HTTP `403`.

### `POST /api/create-table`

Create a new table. Blocked on read-only environments. Only `CREATE TABLE` / `CREATE EXTERNAL TABLE` prefixes allowed.

### `POST /api/add-column`

Add a column to an existing table. Only `ALTER TABLE` prefix allowed.

### `POST /api/admin/exec`

Execute cluster admin commands. Whitelisted prefixes: `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER/BACKEND`, `ALTER SYSTEM DECOMMISSION BACKEND`, `DROP TABLE`, `CREATE DATABASE`, `DROP DATABASE`.

### `POST /api/routine-load/execute`

Manage routine load jobs. Whitelisted: `CREATE/PAUSE/RESUME/STOP/ALTER ROUTINE LOAD`, `SHOW ROUTINE LOAD`.

### `POST /api/batch-load/execute`

Execute batch load commands. Whitelisted: `LOAD LABEL`, `INSERT INTO`, `SHOW LOAD`.

**Request:**
```json
{
  "sql": "LOAD LABEL `mydb`.`my_load_label` (...) WITH BROKER PROPERTIES (...)"
}
```

### `POST /api/batch-load/cancel`

Cancel a batch load job by label.

**Request:**
```json
{
  "database": "mydb",
  "label": "my_load_label"
}
```

### `POST /api/stream-load`

Upload a local file via HTTP Stream Load. This endpoint proxies the file to StarRocks FE HTTP API. Accepts `multipart/form-data`.

| Field | Type | Required | Description |
|---|---|---|---|
| `database` | string | yes | Target database |
| `table` | string | yes | Target table |
| `file` | file | yes | File to upload (max 512 MB) |
| `format` | string | no | `csv`, `json`, `parquet`, `orc` |
| `column_separator` | string | no | Column delimiter (default: `\t`) |
| `row_separator` | string | no | Row delimiter |
| `enclose` | string | no | Enclosure character for quoted fields (e.g. `"`) |
| `escape` | string | no | Escape character within enclosed fields (e.g. `\`) |
| `columns` | string | no | Column mapping |
| `label` | string | no | Unique load label |
| `partial_update` | string | no | `true` for partial column updates |
| `max_filter_ratio` | string | no | Max error ratio (e.g. `0.1`) |
| `strict_mode` | string | no | `true` / `false` |
| `timezone` | string | no | Timezone for conversions |
| `jsonpaths` | string | no | JSON column paths |
| `json_root` | string | no | JSON root path |
| `strip_outer_array` | string | no | `true` to strip outer JSON array |
| `skip_header` | string | no | Number of header rows to skip |
| `trim_space` | string | no | `true` to trim spaces around fields |

```bash
curl -X POST http://localhost:8081/api/stream-load \
  -H "Authorization: Bearer your-admin-token-here" \
  -H "X-Environment: test" \
  -F "database=mydb" \
  -F "table=events" \
  -F "file=@/path/to/data.csv" \
  -F "format=csv" \
  -F "column_separator=,"
```

**Response:** Returns the StarRocks Stream Load response as-is (JSON with Status, Message, NumberTotalRows, etc.)

### `POST /api/kill-query`

Kill a running query by connection ID.

---

## Endpoint Summary

| Method | Path | Auth | Role | Description |
|---|---|---|---|---|
| GET | `/api/health` | - | - | Liveness probe |
| GET | `/api/ready` | - | - | Readiness probe (pings all DB pools) |
| POST | `/api/login` | - | - | Authenticate and get token |
| GET | `/api/docs` | - | - | Scalar API reference UI |
| GET | `/api/docs/openapi.yaml` | - | - | OpenAPI 3.0 spec |
| GET | `/api/environments` | token | all | List environments |
| POST | `/api/query` | token+env | all | Execute SQL (streaming NDJSON) |
| POST | `/api/export` | token+env | all | Export results (CSV/Excel/JSON file) |
| GET | `/api/nodes` | token+env | all | Cluster node health |
| GET | `/api/routines` | token+env | all | All routine load jobs |
| GET | `/api/ingestion?db=` | token+env | all | Routine loads for a database |
| GET | `/api/databases` | token+env | all | List databases |
| GET | `/api/tables?db=` | token+env | all | List tables in database |
| GET | `/api/columns?db=&table=` | token+env | all | Column metadata |
| GET | `/api/table-details?db=` | token+env | all | Detailed table info (tables, views, MVs) |
| GET | `/api/show-create` | token+env | all | DDL for table/view/MV |
| GET | `/api/routine-load/list` | token+env | all | Routine loads by db/table |
| GET | `/api/batch-load/list` | token+env | all | Batch load jobs |
| GET | `/api/process-list` | token+env | all | Running processes |
| GET | `/api/current-queries` | token+env | all | Active queries (detailed) |
| GET | `/api/slow-queries` | token+env | all | Slow query log |
| POST | `/api/create-table` | token+env | admin | Create new table |
| POST | `/api/add-column` | token+env | admin | Add column to table |
| POST | `/api/admin/exec` | token+env | admin | Admin commands |
| POST | `/api/routine-load/execute` | token+env | admin | Manage routine loads |
| POST | `/api/batch-load/execute` | token+env | admin | Execute batch load |
| POST | `/api/batch-load/cancel` | token+env | admin | Cancel batch load |
| POST | `/api/stream-load` | token+env | admin | HTTP file upload (Stream Load) |
| POST | `/api/kill-query` | token+env | admin | Kill a running query |

---

## Architecture

```
Client (curl / Python / Go / Browser)
  |
  +-- Authorization: Bearer <token>
  +-- X-Environment: prod
  |
  v
+----------------------------------+
|  Go HTTP Server (chi router)     |
|  +-- Logging middleware          |
|  +-- CORS middleware             |
|  +-- RequireAuth middleware      |
|  +-- RequireEnv middleware       |
|  +-- RequireAdmin middleware     |
+---------------+------------------+
                |
                v
+----------------------------------+
|  Pool Manager                    |
|  map[envName] -> *sql.DB         |
|                                  |
|  Each pool connects to one or    |
|  more StarRocks FE nodes via     |
|  MySQL protocol (port 9030).     |
|                                  |
|  Multi-FE failover is handled    |
|  at the driver level via         |
|  comma-separated DSN addresses.  |
+---------------+------------------+
                |
                v
+----------------------------------+
|  StarRocks FE Cluster            |
|  fe1:9030, fe2:9030, fe3:9030    |
|  fe1:8030 (HTTP - Stream Load)   |
|         |                        |
|         v                        |
|  StarRocks BE Cluster            |
|  be1, be2, be3, ...              |
+----------------------------------+
```

### Key Design Decisions

- **Streaming-first:** Query results are streamed row-by-row as NDJSON. O(1) memory per request regardless of result size.
- **Stateless:** No sessions, no server-side auth state. Horizontally scalable behind a load balancer.
- **Connection pooling:** One `*sql.DB` pool per environment, with configurable pool sizes and connection recycling.
- **Role-based access:** Admin/viewer roles enforced at the middleware level. Environment-level `read_only` flag provides an additional safety layer.
- **Stream Load proxy:** File uploads are proxied to StarRocks FE HTTP port with auth forwarding and redirect handling.

---

## Security

Checks run before the initial public release (November 2026):

- [x] `go build ./...` and `go vet ./...` pass
- [x] [`govulncheck`](https://go.dev/doc/security/vuln/) reports no known vulnerabilities reachable from this code (Go 1.26.11)
- [x] Source tree scanned for committed secrets, config files with credentials and internal hostnames; none found
- [ ] The project has **no automated tests** yet

`github.com/xuri/excelize/v2` is pinned to an untagged upstream commit (`v2.11.1-0.20260915055537-22f76f9acb94`) because the fixes for several advisories are not in a tagged release yet. Move to the next tagged release when it is available.

These checks are a point-in-time snapshot, not a security audit. Read the [Deployment notes in SECURITY.md](SECURITY.md#deployment-notes) before exposing the API. To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
