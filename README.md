# AirLake

AirLake is an air-quality data pipeline project built around OpenAQ data and Apache Kafka.

The project currently focuses on:

- OpenAQ API diagnostics and metadata inspection
- Air-quality data freshness checks
- Local historical air-quality data stored as compressed CSV files
- Publishing historical measurements to Apache Kafka

## Architecture

```text
OpenAQ API
    │
    ├── Latest-data diagnostics
    ├── Location search
    ├── Freshness checks
    └── Location metadata / license inspection

Historical CSV.GZ data
    │
    ▼
real_producer.py
    │
    ▼
Kafka broker
    │
    ▼
aq_measurements

The current repository contains the producer side of the Kafka pipeline.
A Kafka consumer and downstream processing layer are not included yet.
Requirements
- macOS or Linux
- Python 3.12
- uv
- Apache Kafka
- An OpenAQ API key
The project uses .python-version to specify Python 3.12 and uv to manage the project environment and dependencies.
Dependencies
Runtime dependencies:
- requests
- confluent-kafka
The resolved dependency graph is recorded in uv.lock.
Setup
1. Clone the repository
If you are setting up AirLake on a new machine:
git clone git@github.com:careych/AirLake.git
cd AirLake

2. Install Python 3.12
If uv is already installed:
uv python install 3.12

The repository contains .python-version, so the project is configured to use Python 3.12.
3. Create the project environment
uv sync

This creates or updates the project's virtual environment and installs the project dependencies.
4. Configure the OpenAQ API key
Create a local environment file:
cp .env.example .env

Then set your API key:
OPENAQ_KEY=your_open_aq_api_key

The .env file is intentionally excluded from Git.
Load the variables into the current shell before running the OpenAQ scripts:
set -a
source .env
set +a

Never commit or share the real API key.
OpenAQ diagnostics
check_latest.py
Checks the latest measurements for selected OpenAQ locations.
uv run python check_latest.py

check_license.py
Inspects location metadata including available license, owner, and provider information.
uv run python check_license.py

License information may differ between data sources and locations.
check_openaq.py
Searches for air-quality monitoring locations around Hanoi, checks PM2.5 freshness, and reports API rate-limit information.
uv run python check_openaq.py

freshness_report.py
Reports the age of recent measurements for selected locations and sensors.
uv run python freshness_report.py

probe_locations.py
Probes selected locations and PM2.5 sensor measurements.
uv run python probe_locations.py

Kafka producer
real_producer.py reads compressed historical CSV files from:
data/raw/

and publishes the measurements to Apache Kafka.
The current default Kafka broker is:
localhost:9092

The producer writes to:
aq_measurements

Run:
uv run python real_producer.py

An optional positional argument controls the delay between batches of 2,000 records:
uv run python real_producer.py 0.1

The Kafka message key is:
location_id:sensors_id

The message value is the original CSV row serialized as JSON.
A Kafka broker must be running locally before executing the producer.
Data
The local historical dataset is organized approximately as:
data/
└── raw/
    ├── 4946812/
    │   └── year=YYYY/
    │       └── month=MM/
    ├── 4946813/
    │   └── year=YYYY/
    │       └── month=MM/
    └── 6123215/
        └── year=YYYY/
            └── month=MM/

Raw data is intentionally excluded from Git and is not redistributed through this repository.
License and attribution information may vary by data source and location. Check the corresponding OpenAQ metadata before redistributing data.
Project structure
AirLake/
├── .env.example
├── .gitignore
├── .python-version
├── pyproject.toml
├── uv.lock
├── README.md
│
├── check_latest.py
├── check_license.py
├── check_openaq.py
├── freshness_report.py
├── probe_locations.py
└── real_producer.py

The local data/ directory is intentionally excluded from Git.
Development
Run project Python commands through uv:
uv run python <script>.py

Compile-check the Python scripts:
uv run python -m compileall -q .

Synchronize dependencies:
uv sync

Security
Never commit API keys, credentials, or other secrets.
Local-only files include:
.env
.venv/
data/

Use .env.example as the configuration template.
Current status
The current project provides:
1. OpenAQ location and measurement diagnostics
2. Measurement freshness inspection
3. Local historical air-quality data
4. Kafka-based historical data ingestion
Future work may include:
- Kafka consumer
- Stream processing
- Data storage
- Data quality validation
- Workflow orchestration
- Cloud deployment
- Analytics and visualization
