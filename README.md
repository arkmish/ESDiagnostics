# ElasticSearch Diagnostics Tool

The **ESDiagnostics** tool connects to an active Elasticsearch cluster (or reads from a previously exported diagnostics `.zip` dump) and evaluates its health, resource utilization, and internal configurations against best practices.

It generates an intuitive **HTML Dashboard Report** full of visual indicators, pie charts, and deep-dive tables to give context on what is failing and why, while providing actionable suggestions.

## 🚀 Setup & Installation

### Option 1: Automatic Setup via Venv (Recommended)
You can automatically create a virtual environment and load all dependencies using the provided bash script:
```bash
# Set up virtual environment and install requirements
sh ./setup_venv.sh

# Activate the environment
source venv/bin/activate
```

### Option 2: Manual Setup
This project has recently been refactored for Python 3.9+ and modernized dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 💻 Usage

The tool can function in two modes: analyzing a pre-collected zipped dataset or live-querying the cluster. 

### 1. Offline Analysis (From Zip)
First, you can gather statistics directly from an Elasticsearch Node asynchronously. This is useful for secured production environments where you cannot link an outside utility. 
```bash
# Capture the stats to a ZIP file from any Linux node
sh ./getStatsFromES.sh "https://<ES_URL>" "username" "password"
```

Once you have the `.zip` file, analyze it locally:
```bash
python3 es_search.py -CanAccessCluster False -StatsZipFile /tmp/PSR_ESDiagDump_cluster...zip
```

### 2. Live Analysis (Direct Connection)
If your machine has direct connectivity to your Elasticsearch cluster, analyze it natively. You can use command arguments, or set secure environment variables.

Using Environment Variables (Safest):
```bash
export ES_USERNAME="elastic"
export ES_PASSWORD="my_secure_password"
python3 es_search.py -CanAccessCluster True -secure True -URL "https://<ES_URL>:9200"
```
Using CLI Arguments:
```bash
python3 es_search.py -CanAccessCluster True -secure True -URL "https://<ES_URL>:9200" -Username elastic -password secret_pass
```

---

## 🏗 Modular Architecture

The original monolith has been rewritten into several robust components under the `src/` directory to enhance extensibility:
- **`data_loader.py`**: Interacts with the `elasticsearch` native bindings natively or seamlessly simulates data extraction dynamically from a cluster dump ZIP.
- **`checkers_os.py`**: Runs evaluations focusing on raw underlying Node/Host server capabilities (CPU, Memory limits, Swap configuration).
- **`checkers_es.py`**: Broad operational validations specific to Elastic (Shard counts, shard distributions, circuit breakers, segment fragmentation, dedicated master limits).
- **`checkers_config.py`**: Analyzes the static and transient settings applied to the cluster. Validates regex blocks, rebalancing options, scroll query limitations etc.
- **`checkers_stats.py`**: Creates data aggregations required by the HTML UI overview.
- **`report.py / template.py`**: Handles generating interactive DataFrames styled through Pandas and rendering the final HTML CSS.
- **`state.py`**: Replaces problematic global variables using an Object-Oriented generic State Tracker (`DiagnosticState`).

---

## 🔎 Rules & Diagnostic Checks Reference

The Tool parses hundreds of variables and applies predefined thresholds. Below are the rules strictly defining failures and warnings:

### 🖥 Operating System Level
*   **CPU Load Average**: Evaluates `15m` load. If `15m` load exceeds `0.7 * CPU_Core_Count`, issues a **WARNING**. If it exceeds `0.8 * CPU_Core_Count`, issues a **FAIL**. 
*   **Memory Usage**: If consumed OS level memory reaches *85% to 90%*, logs a **WARNING**. *Over 90%* triggers a **FAIL**. 
*   **Disk Usage**: Alerts for disks crossing **80%** occupancy (WARNING) and blocks if over **90%** (FAIL).
*   **Disk Distribution**: Verifies if standard deviation of consumed capacity across all datanodes exceeds `5` (Healthy distribution means shards aren't clogging singular disks).
*   **Swap Control**: Validates memory swap is completely disabled across all nodes. Any consumption triggers a **FAIL**, negatively impacting performance heavily.

### 🌐 ES Cluster Performance & Configuration 
*   **Cluster Health**: Reflects core statuses directly (Green=`PASS`, Yellow=`WARN`, Red=`FAIL`).
*   **Shards Distribution**: Uses standard deviations across current shard deployments to detect node hotspots. 
*   **Total Shards Per Heap Limiter**: Applies the generic Elastic rule restricting you to ~25 shards per GB of memory overhead locally. Failing this usually indicates massive mapping/index sprawl.
*   **Shards Size (Per Index)**: Ensures shards are safely under the 50GB limit. Anything over heavily damages reallocation recovery performance and requires a `_reindex`.
*   **Empty Indices**: Warns if <=5 blank indices exist, fails heavily if there's >5 indicating orphaned logging rotation footprints wasting file descriptors.
*   **Slow Tasks Evaluator**: Checks running/pending background tasks. Active tasks taking over *5 seconds* are flagged, tasks exceeding *10 seconds* result in a **FAIL**.
*   **Dedicated Master Verification (Split-Brain Prev)**: Checks that if a cluster exceeds 10 Data Nodes, there should definitively be 3 separate master-eligible nodes to form solid quorums.
*   **Memory Footprints Configurations**: 
    * Dedicated Master node heap limits (requires 4-6GB optimally, otherwise raises warnings).
    * Data Node limits (Identifies if they run the optimal 20~30GB sweet-spot limit).
*   **Circuit Breakers**: Evaluates memory allocations reserved for heavy aggregations. Exceeding 80% circuit-breaker threshold risks sudden Node OOM crashes resulting in **FAIL**.
*   **Segment Fragmentation**: Tracks deleted memory-documents stored invisibly before lucene merges run. If fragmentation exceeds 40-50%, warns against wasted caching segments.
*   **Open File Descriptors**: Ensures connection scaling limits have appropriate Unix system headroom overhead. Exceeding 50% max open files raises a **FAIL**.

### ⚙ Cluster Runtime Limitations 
*   **Allocation / Rebalancing Flags**: Assures cluster rebalancing hasn't been purposefully left off / stalled preventing recovery.
*   **Costly Wildcard Flags**: Determines if `allowLeadingWildcard` or `destructive_requires_name` flags are mistakenly enabled, which permit dangerously expensive query patterns (`*name*`).
*   **Painless Regex Execution**: Checks if raw regex was accidentally allowed on scripted evaluations (highly compute-intensive).
*   **Concurrent Node Recoveries**: Checks against conservative throttling that might dramatically delay replacing a broken node.

---

## 🧪 Development & Testing

This project incorporates Flake8 PEP8-compliant automated formatting rules alongside a comprehensive `pytest` Unit-Test ecosystem.

To run tests ensuring code-quality:
```bash
# Validate python syntax formatting standards
flake8 src/ es_search.py

# Execute automated logic assertions
pytest tests/
```