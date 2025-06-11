# ⚡ Kafka Crypto Stream with S3-Triggered Lambda

A real-time data pipeline that streams cryptocurrency price data using a Kafka **Producer-Consumer** setup, stores it in AWS S3 in Parquet format, and processes it using AWS Lambda, Glue, and Athena for querying and analysis.

---

## 🔁 Data Flow Overview

1. A **Kafka Producer** fetches real-time cryptocurrency data from an external API (e.g., CoinGecko, Binance).
2. The data is sent to a **Kafka topic**.
3. A **Kafka Consumer** listens to the topic and writes the data to **S3** in **Parquet** format.
4. **S3 PUT event** triggers an **AWS Lambda** function.
5. The Lambda function starts an **AWS Glue Crawler**, which updates the Glue Data Catalog.
6. **Athena** is used to query the processed data.

---

## 🏭 Kafka Producer

- Connects to a public crypto API at fixed intervals (e.g., every 5 seconds)
- Publishes price data to a Kafka topic like `crypto-prices`
- Written in Python using `kafka-python`

```python
# Import requied libraries
import websocket
import json
import time 
from kafka import KafkaProducer
from datetime import datetime

# Kafka Producer config
producer = KafkaProducer(
    bootstrap_servers='35.154.244.134:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = 'crypto_ticker_stream'
```

---
## 📥 Kafka Consumer 
- Consumes data from crypto-topic.
- Converts to Parquet format.
- Uploads files to S3 in batches.

```python
# Import requied libraries
from kafka import KafkaConsumer
import pandas as pd
import s3fs
import json
from datetime import datetime

# Kafka consumer config
consumer = KafkaConsumer(
    'crypto_ticker_stream',
    bootstrap_servers=['35.154.244.134:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
---
## 🧠 Athena Query Example

```sql
SELECT symbol, COUNT(*) AS total_records
FROM crypto_kafka.akshay_test1_bucket
GROUP BY symbol
ORDER BY total_records DESC
```
---

## 📸 Screenshots

### 1. Kafka Producer & Consumer Running
![Kafka Consumer](assets/kafka_consumer_terminal.png)

### 2. Parquet File Uploaded to S3
![S3 Upload](assets/s3_parquet_upload.png)

### 3. Lambda Triggered by S3 Event
![Lambda Trigger](assets/lambda_trigger.png)

### 4. Glue Crawler Configuration
![Glue Crawler](assets/glue_crawler.png)

### 5. Querying Data in Athena
![Athena Results](assets/athena_query_results.png)

---

## 📦 Technologies Used

- **Kafka** – Real-time streaming (Producer + Consumer)
- **AWS S3** – Scalable storage for Parquet files
- **AWS Lambda** – Trigger to start Glue crawler
- **AWS Glue** – Data catalog & schema detection
- **Amazon Athena** – SQL-on-S3 query engine

---


