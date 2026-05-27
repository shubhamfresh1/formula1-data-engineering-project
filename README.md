# Formula 1 Data Engineering Project

## Architecture
[image: Bronze → Silver → Gold diagram]

## Tech Stack
Azure Databricks | PySpark | Azure Data Factory | ADLS Gen2 | Delta Lake

## Project Overview
...

## Pipeline Stages
### Bronze Layer — Raw Ingestion
### Silver Layer — Cleansed & Validated
### Gold Layer — Aggregated Analytics

## Analytical Outputs
- Driver Standings
- Constructor Rankings
- Lap Time Analytics

## How to Run
...
## ⚙️ How to Run

### Prerequisites

Before running this project, make sure you have the following set up:

| Requirement | Details |
|---|---|
| Azure Account | Free tier or Pay-as-you-go |
| Azure Databricks | Standard or Premium tier workspace |
| Azure Data Lake Storage Gen2 | Storage account with hierarchical namespace enabled |
| Azure Data Factory | For pipeline orchestration |
| Ergast F1 API | Free, no API key required → [ergast.com/mrd](http://ergast.com/mrd/) |

---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/shubhamfresh1/formula1-data-engineering-project.git
```

---

### Step 2 — Set Up Azure Data Lake Storage Gen2

1. Go to **Azure Portal → Storage Accounts → Create**
2. Enable **Hierarchical Namespace** (required for ADLS Gen2)
3. Create the following containers:

```
formula1dl/
├── raw/          ← Bronze Layer (raw ingested data)
├── processed/    ← Silver Layer (cleansed & validated)
└── presentation/ ← Gold Layer (analytics-ready tables)
```

4. Note down your **Storage Account Name** and **Access Key**

---

### Step 3 — Configure Azure Databricks

1. Launch your **Azure Databricks workspace**
2. Go to **Compute → Create Cluster** with the following settings:
   - Runtime: **13.3 LTS (Spark 3.4, Scala 2.12)** or higher
   - Node type: `Standard_DS3_v2` (or equivalent)
3. Mount ADLS Gen2 to Databricks — run this in a notebook cell:

```python
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<your-client-id>",
  "fs.azure.account.oauth2.client.secret": "<your-client-secret>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://raw@<storage-account>.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/raw",
  extra_configs = configs
)
```

> 💡 Repeat the mount for `processed/` and `presentation/` containers.

---

### Step 4 — Import Notebooks into Databricks

1. In Databricks, go to **Workspace → Import**
2. Select **URL or File**, upload the `Formula1/` folder contents from this repo
3. The notebooks will appear in your workspace under the `Formula1/` directory

---

### Step 5 — Run Notebooks in Order

Execute the notebooks in the following sequence:

```
1. ingestion/         ← Ingest raw F1 data from Ergast API → Bronze Layer
2. trans/             ← Transform and validate data        → Silver Layer
3. gold/              ← Aggregate for analytics            → Gold Layer
```

> Each notebook can be run manually via **Run All**, or triggered via Azure Data Factory (see Step 6).

---

### Step 6 — Set Up Azure Data Factory Pipelines (Optional)

To automate and schedule the pipeline:

1. Go to **Azure Data Factory → Author → Pipelines → New Pipeline**
2. Add a **Databricks Notebook activity** for each layer (Bronze → Silver → Gold)
3. Link activities with **dependencies** (Silver runs only after Bronze succeeds)
4. Set a **trigger** (e.g., daily at midnight) under **Manage → Triggers**
5. Publish and **enable the trigger**

---

### Step 7 — Verify Analytical Outputs

Once all notebooks complete, verify the Gold Layer tables in Databricks:

```python
# Check available Gold Layer tables
display(spark.sql("SHOW TABLES IN f1_presentation"))

# Sample output queries
spark.sql("SELECT * FROM f1_presentation.driver_standings LIMIT 10").show()
spark.sql("SELECT * FROM f1_presentation.constructor_standings LIMIT 10").show()
spark.sql("SELECT * FROM f1_presentation.calculated_race_results LIMIT 10").show()
```

---

### Project Structure

```
formula1-data-engineering-project/
│
├── Formula1/
│   ├── ingestion/       # Bronze Layer — Raw data ingestion notebooks
│   ├── trans/           # Silver Layer — Transformation & cleansing notebooks
│   ├── gold/            # Gold Layer — Aggregation & analytics notebooks
│   └── includes/        # Shared utilities & configuration
│
└── manifest.mf          # Databricks workspace manifest
```
