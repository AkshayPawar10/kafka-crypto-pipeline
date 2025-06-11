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

https://github.com/user-attachments/assets/8a292dae-8602-4611-8dca-e3eedc584d27
```python

from kafka import KafkaProducer
import requests, json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def get_crypto_data():
    response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd')
    return json.dumps(response.json()).encode('utf-8')

while True:
    data = get_crypto_data()
    producer.send('crypto-prices', value=data)
```

---

## 🧠 Athena Query Example

```sql
SELECT symbol, COUNT(*) AS total_records
FROM crypto_kafka.akshay_test1_bucket
GROUP BY symbol
ORDER BY total_records DESC;
```


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


