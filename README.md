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
```
---
## 🧠 Lambda Trigger
Triggered by S3 upload to run Glue Crawler

```python
import boto3
def lambda_handler(event, context):
    glue = boto3.client('glue')
    response = glue.start_crawler(Name='crypto-kafka-project')  

    return {
        'statusCode': 200,
        'body': f'Started Glue Crawler: {response}'
    }
```
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

### 1. Zookeeper and kafka running
![Zookeeper and Kafka](https://github.com/user-attachments/assets/e0d71009-ae26-45b1-8314-ce17f58a3c27)

### 2. Producer and Consumer running 
![producer](https://github.com/user-attachments/assets/2ec044a1-80a8-489a-8090-8fc87c3bde22)
![consumer](https://github.com/user-attachments/assets/79db1ba5-e1eb-4c85-ad26-9e4206c2a4be)

### 3. Parquet File Uploaded to S3
![S3](https://github.com/user-attachments/assets/8d3b9fc0-62de-4066-ab16-2d11c4ada023)

### 4. Lambda Triggered by S3 Event
![lambda_1](https://github.com/user-attachments/assets/1b92f040-e552-467b-8cef-f92c20767da4)

### 5. Glue Crawler Configuration
![crawler](https://github.com/user-attachments/assets/3310c447-d90d-4da0-9144-dafcb4c85f45)

### 6. Querying Data in Athena
![Athena query](https://github.com/user-attachments/assets/9a1a50ce-2843-48d7-a0a5-91f17b842891)

---

## 📦 Technologies Used

- **Kafka** – Real-time streaming (Producer + Consumer)
- **AWS S3** – Scalable storage for Parquet files
- **AWS Lambda** – Trigger to start Glue crawler
- **AWS Glue** – Data catalog & schema detection
- **Amazon Athena** – SQL-on-S3 query engine

---


