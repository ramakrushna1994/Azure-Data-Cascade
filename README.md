# Azure Data Cascade

Real-time medallion pipeline on Azure Databricks. Ingests from Confluent Cloud (Kafka),
REST APIs, and ADLS Gen2 files into Delta Lake, governed by Unity Catalog.

```
Confluent Kafka ─┐
REST API ────────┼─→ Bronze (raw) ─→ Silver (clean, SCD2) ─→ Gold (aggregates) ─→ BI views
ADLS files ──────┘                         │
                                     quarantine
```

## Architecture

| Layer | Contents | Partitioned by |
|---|---|---|
| **Bronze** | Raw rows plus ingestion metadata and Kafka offsets | `_bronze_ingestion_date` (DATE) |
| **Silver** | Normalized, validated, deduplicated, SCD Type 2 history; rejects quarantined | `order_date` (DATE) |
| **Gold** | Daily / hourly / per-customer aggregates, date dimension, BI views | `order_date` (DATE) |

All tables live in one Unity Catalog catalog across the schemas `bronze_layer`,
`silver_layer`, `gold_layer`, `monitoring`, and `analytics`.

## Layout

```
pipeline_config.py    Shared config: UC naming, secret expansion, storage paths.
                      A workspace FILE, not a notebook, so notebooks can import it.

bronze_layer.py       Notebook. Bronze DDL + ingestion metrics.
kafka_consumer.py     Notebook. Confluent -> Bronze via Structured Streaming.
api_ingestion.py      Notebook. REST polling -> Bronze.
file_ingestion.py     Notebook. Auto Loader -> Bronze.
silver_layer.py       Notebook. Normalize, validate, dedupe, SCD Type 2.
gold_layer.py         Notebook. Aggregates, date dimension, analytics views.
quality_checks.py     Notebook. Config-driven data quality rules.
monitoring.py         Notebook. Freshness SLAs, processing lag, health report.

kafka_producer.py     Local script. Sends sample orders to the topic.
config.yaml           Catalog, storage paths, Kafka, quality rules, SLAs.
jobs_config.json      One multi-task Databricks job. No schedule.
```

The eight pipeline modules are Databricks **notebooks** (`# Databricks notebook source`
with `# COMMAND ----------` cells). `pipeline_config.py` is deliberately a plain file —
notebooks cannot be imported as modules, only `%run`, which would scatter configuration
across the global namespace.

## Configuration

`config.yaml` contains no secrets. `${VAR}` placeholders are expanded from the
environment by `pipeline_config.load_config()`, and `require()` raises a clear error if a
placeholder survives — rather than passing the literal string `"${CONFLUENT_API_SECRET}"`
to a broker and getting an opaque auth failure.

Table names are never written by hand. `cfg.table("silver", "silver_orders")` resolves to
`<catalog>.silver_layer.silver_orders`, so changing catalog is a one-line config edit.

On Databricks the job injects secrets as cluster environment variables:

```json
"spark_env_vars": {
  "CONFLUENT_API_KEY": "{{secrets/cascade/confluent_api_key}}"
}
```

## Setup

### Prerequisites

- Azure Databricks **Premium** workspace with Unity Catalog
- ADLS Gen2 storage account (**hierarchical namespace enabled** — this cannot be turned
  on after creation)
- A Confluent Cloud **Basic** cluster (no base fee) or Azure Event Hubs
- Databricks CLI v0.2xx+ (`winget install Databricks.DatabricksCLI`). The old
  `databricks-cli` PyPI package (≤0.18) is deprecated and its syntax is incompatible.

### 1. Authenticate

Reuse your Azure login rather than managing a PAT:

```bash
az login
export DATABRICKS_HOST="https://adb-<workspace-id>.<n>.azuredatabricks.net"
export DATABRICKS_AUTH_TYPE="azure-cli"
databricks current-user me
```

### 2. Storage access (no account keys)

Create an **Access Connector**, grant its managed identity **Storage Blob Data
Contributor** on the storage account, then register it with Unity Catalog:

```bash
databricks storage-credentials create <credential> \
  --json '{"azure_managed_identity": {"access_connector_id": "<connector resource id>"}}'

databricks external-locations create cascade_root \
  "abfss://<container>@<account>.dfs.core.windows.net/adb/cascade" <credential>
```

Run **Test connection** on the external location in the Catalog UI. That validates the
whole chain and is far easier to debug than a failing Spark job.

### 3. Catalog and schemas

```bash
databricks catalogs create cascade \
  --storage-root "abfss://<container>@<account>.dfs.core.windows.net/adb/cascade/catalog"

for s in bronze_layer silver_layer gold_layer monitoring analytics; do
  databricks schemas create "$s" cascade
done
```

Do not assume a `main` catalog exists — many metastores have none. Whatever you use goes
in `config.yaml` under `databricks.catalog`; no code hardcodes a catalog name.

### 4. Secrets

```bash
databricks secrets create-scope cascade
databricks secrets put-secret cascade confluent_bootstrap  --string-value "pkc-xxxxx...:9092"
databricks secrets put-secret cascade confluent_api_key    --string-value "..."
databricks secrets put-secret cascade confluent_api_secret --string-value "..."
```

Use a **cluster-scoped** Confluent API key. A Cloud API key manages control-plane
resources and cannot authenticate a Kafka connection.

### 5. Deploy

```bash
databricks workspace mkdirs /Shared/cascade
for f in *.py config.yaml; do
  databricks workspace import "/Shared/cascade/$f" --file "$f" --format AUTO --overwrite
done
databricks workspace list /Shared/cascade
```

`pipeline_config.py` and `config.yaml` must show as type **FILE**; the eight modules show
as **NOTEBOOK** with the `.py` stripped — which is why `jobs_config.json` references them
without an extension.

Keep the `.py` on the *target* path. `--format AUTO` only detects the notebook header when
the target keeps its extension; without it the file uploads as FILE and `notebook_task`
cannot run it.

### 6. Run

`config.yaml` and `jobs_config.json` carry `${ADLS_ACCOUNT}` / `<your-adls-account>`
placeholders so no environment detail is committed. Your real values live in `.env`,
which is gitignored:

```bash
cp .env.example .env     # then fill in your values
```

`pipeline_config` loads `.env` automatically for local runs (tests,
`kafka_producer.py`). Deploying is a shell step, so export it there:

```bash
set -a; . ./.env; set +a

# Fail loudly rather than substituting an empty string and producing
# an invalid abfss:// path that only breaks at runtime.
: "${ADLS_ACCOUNT:?set ADLS_ACCOUNT in .env}"
: "${ADLS_CONTAINER:?set ADLS_CONTAINER in .env}"

sed -e "s|<your-adls-account>|$ADLS_ACCOUNT|" \
    -e "s|<your-container>|$ADLS_CONTAINER|" \
    jobs_config.json > /tmp/job.json

databricks jobs create --json @/tmp/job.json
databricks jobs run-now <job-id>
```

The job stores `ADLS_ACCOUNT` and `ADLS_CONTAINER` as cluster environment variables,
and `pipeline_config` expands them into the `abfss://` paths at runtime. Note these are
**not credentials** — access is granted by the Unity Catalog external location and its
Access Connector managed identity, so the account name alone grants nobody anything.

The job has **no schedule** — it runs when you trigger it, or from an external
orchestrator. Nothing starts a cluster on a timer.

## Task graph

```
bronze_setup → kafka_ingest → silver_transform → gold_aggregate → quality_checks
                                                                → monitoring
```

One multi-task job on one shared cluster, so each task genuinely waits for its upstream.
`quality_checks` fails the run when a rule fails, which is what makes the job history
mean something.

## Two path namespaces

Easy to conflate, and the failure modes are unhelpful:

| Setting | Namespace | Example |
|---|---|---|
| `notebook_path` | workspace **object** path | `/Shared/cascade/bronze_layer` |
| `CASCADE_CONFIG` | filesystem, via FUSE mount | `/Workspace/Shared/cascade/config.yaml` |

Using the object path for the config file yields `FileNotFoundError` at import time.

## Networking

If the workspace is **VNet-injected with No-Public-IP**, cluster VMs have no outbound
internet route unless a NAT Gateway or a UDR to a firewall exists. ADLS keeps working —
Databricks pre-provisions NSG rules for the `Storage`, `Sql`, `EventHub` and
`AzureDatabricks` service tags, which travel the Azure backbone — but a public endpoint
like Confluent Cloud is unreachable, surfacing as `Failed to create new KafkaAdminClient`.

```bash
az databricks workspace show -n <ws> -g <rg> \
  --query "{npip:parameters.enableNoPublicIp.value, vnet:parameters.customVirtualNetworkId.value}"

az network vnet subnet list -g <rg> --vnet-name <vnet> \
  --query "[].{name:name, natGateway:natGateway.id, routeTable:routeTable.id}"
```

| Option | Cost | Trade-off |
|---|---|---|
| Auto Loader on ADLS | $0 | No broker; ADLS needs no internet |
| Azure Event Hubs | ~$20/mo | NSG rule for `EventHub:9093` already present |
| NAT Gateway + Confluent | ~$32/mo | Billed 24/7 regardless of activity |
| Disable No-Public-IP | ~$0 | Public IPs on compute; weaker posture |

### Databricks shades its Kafka client

DBR relocates the Kafka client into the `kafkashaded` package. JAAS resolves the login
module by string, so the unshaded class name is not on the classpath:

```
kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
```

The symptom is `Failed to create new KafkaAdminClient`; the real cause is three levels
down — `No LoginModule found for org.apache.kafka.common.security.plain.PlainLoginModule`.
`kafka_consumer.py` selects the right name from `DATABRICKS_RUNTIME_VERSION`, so local
Spark still works.

## Operations

```sql
-- What got rejected and why
SELECT error_message, COUNT(*) FROM cascade.silver_layer.silver_orders_quarantine
GROUP BY error_message;

-- Quality history
SELECT * FROM cascade.monitoring.quality_check_results ORDER BY checked_at DESC;

-- Freshness and processing lag
SELECT * FROM cascade.monitoring.pipeline_metrics ORDER BY measured_at DESC;

-- How far Silver has consumed Bronze
SELECT * FROM cascade.monitoring.silver_watermarks;
```

Silver processes incrementally against the watermark table. To force a full reprocess,
delete the relevant row from `silver_watermarks` and rerun.

Delta time travel covers data mistakes:

```sql
DESCRIBE HISTORY cascade.silver_layer.silver_orders;
RESTORE TABLE cascade.silver_layer.silver_orders VERSION AS OF <n>;
```

## Tests

```bash
pytest tests/ -q          # 33 tests
```

Two groups. `test_pipeline_config.py` needs no Spark and runs in under a second —
placeholder expansion, `require()` rejecting unexpanded `${VAR}`, three-part table names,
and checkpoint/schema paths not colliding. `test_transforms.py` uses a local
SparkSession and covers the Silver logic: uppercase status passing validation,
unparseable dates quarantining rather than becoming NULL, each failure mode getting its
own `error_message`, deduplication keeping the newest row per key, and `order_timestamp`
being carried through for the hourly rollup.

The pure transforms live in `transforms.py` rather than inside `silver_layer.py`,
because a notebook executes its run cells on import and so cannot be imported by a test.

**Python 3.11 or 3.12.** PySpark 3.5 does not support 3.13+ — on 3.14 it fails with
`PicklingError` from cloudpickle. Databricks Runtime 16.4 uses Python 3.12. A JRE is also
required (Java 17 works). The transform tests skip automatically if pyspark is absent, so
the config tests still run anywhere.

```bash
py -3.11 -m venv .venv && .venv/Scripts/pip install -r requirements.txt
```

## Sending test data

```bash
pip install -r requirements.txt
export CONFLUENT_BOOTSTRAP="pkc-xxxxx...:9092"
export CONFLUENT_API_KEY="..."
export CONFLUENT_API_SECRET="..."

python kafka_producer.py --count 20
```

The producer deliberately emits four invalid records — negative amount, null user,
unparseable date, unknown status — one per quarantine path. A correct run puts valid rows
in `silver_orders` and exactly those four in `silver_orders_quarantine`.

## Costs

- **Confluent Basic** — no base fee; ~$0 at low volume. Standard and Enterprise carry
  ~$385 and ~$895/month base fees, so verify the tier.
- **Databricks** — one single-node job cluster, only while a run is active.
- **ADLS** — negligible at this volume.

## Teardown

```bash
databricks jobs delete <job-id>
databricks secrets delete-scope cascade
databricks catalogs delete cascade --force
databricks external-locations delete cascade_root
```

The storage account, Access Connector, and storage credential survive and must be removed
separately.

## Contributing

One concern per branch, opened as a PR — `main` is protected against direct pushes and
force pushes.

## License

Apache 2.0. See [LICENSE](LICENSE).
