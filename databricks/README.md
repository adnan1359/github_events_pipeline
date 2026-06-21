# Databricks Setup (Free Edition / Serverless)

Databricks **Free Edition** runs on **serverless** compute with **Unity Catalog** on.
That changes a few things vs. the old Community Edition:

| Thing                  | Free Edition reality                                            |
|------------------------|-----------------------------------------------------------------|
| Compute                | Serverless only — no classic clusters, no `cluster_id`          |
| Streaming trigger      | Only `Trigger.availableNow` (no continuous / processingTime)    |
| File access            | No `/dbfs/` FUSE — use **Unity Catalog Volumes** (`/Volumes/…`) |
| Kafka connector        | **Preinstalled** — no Maven library to add                      |
| Namespacing            | 3-level `catalog.schema.table` (UC)                             |
| API auth               | Personal Access Token (PAT) works                               |

Your workspace (from the SQL Warehouse connection screen):

```
Server hostname : dbc-61ca3723-5fde.cloud.databricks.com
SQL Warehouse   : /sql/1.0/warehouses/5b6dcbc812fb1238   (used by dbt + Power BI)
```

> 🔐 **Rotate the access token you shared in chat** (User Settings → Developer →
> Access tokens → revoke + regenerate). Never commit a token; keep it in `.env`
> or a secret scope.

---

## 1. Find your catalog name

Catalog Explorer (left nav) → note the catalog name. On Free Edition it's usually
`workspace`. If yours differs, pass it as the `catalog` widget / `DATABRICKS_CATALOG`.

## 2. Convert the Aiven PEM certs to Java keystores (locally)

Run in the `certs/` folder where you downloaded `ca.pem`, `service.cert`, `service.key`
(needs `keytool` + `openssl`; both ship with a JDK / Git Bash). Pick any passwords:

```bash
cd certs

keytool -import -file ca.pem -alias CA \
  -keystore client.truststore.jks -storepass <TRUSTSTORE_PW> -noprompt

openssl pkcs12 -export -inkey service.key -in service.cert \
  -out client.keystore.p12 -name service -password pass:<KEYSTORE_PW>

keytool -importkeystore \
  -srckeystore client.keystore.p12 -srcstoretype PKCS12 -srcstorepass <KEYSTORE_PW> \
  -destkeystore client.keystore.jks -deststorepass <KEYSTORE_PW>
```

## 3. Create a Volume and upload the keystores

In a Databricks notebook (or SQL editor), create the schema + volume:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.gh_bronze;
CREATE VOLUME IF NOT EXISTS workspace.gh_bronze.pipeline;
```

Then upload the two `.jks` files into the volume:
- **UI:** Catalog Explorer → `workspace` → `gh_bronze` → `pipeline` → **Upload** →
  put them under a `certs/` folder, OR
- **CLI:**
  ```bash
  databricks fs cp client.truststore.jks dbfs:/Volumes/workspace/gh_bronze/pipeline/certs/
  databricks fs cp client.keystore.jks   dbfs:/Volumes/workspace/gh_bronze/pipeline/certs/
  ```

Final paths the Bronze notebook expects:
```
/Volumes/workspace/gh_bronze/pipeline/certs/client.truststore.jks
/Volumes/workspace/gh_bronze/pipeline/certs/client.keystore.jks
```

## 4. Store secrets (passwords + bootstrap only — not the cert files)

```bash
databricks secrets create-scope kafka-secrets

printf 'kafka-3ca0846-adnananam1359-fbe9.f.aivencloud.com:12815' | \
  databricks secrets put-secret kafka-secrets bootstrap-servers
printf '<TRUSTSTORE_PW>' | databricks secrets put-secret kafka-secrets ssl-truststore-password
printf '<KEYSTORE_PW>'   | databricks secrets put-secret kafka-secrets ssl-keystore-password
```

## 5. Import the notebooks

Upload the three files in `notebooks/` to `/Shared/github_pipeline/` (Workspace →
Import). Attach each to **Serverless** compute (Connect → Serverless).

**Run order: 01 → 02 → 03.** Airflow automates all three hourly via the Jobs API
(serverless, no cluster id). `01` uses `availableNow`, so each hourly run drains the
new Kafka messages and stops — cheap and Free-Edition-safe.

---

### If Kafka → serverless gives you trouble
External-Kafka egress from Free Edition serverless usually works, but if you hit
network/credential walls, the fastest unblock is to run **notebook 01 only** against
a short-lived classic cluster on an Azure/AWS **trial** workspace (14-day, $200), and
keep Silver/Gold + dbt + BI on Free Edition. The medallion design doesn't change.
