from kafka import KafkaConsumer
import pandas as pd
import json
import os
from datetime import datetime

def consume_kafka_to_parquet(topic_name, bootstrap_servers, output_directory, batch_size=1000):
    """
    Consume Kafka messages and save to Parquet files in batches
    """
    os.makedirs(output_directory, exist_ok=True)
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='parquet-consumer-group',
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    transactions = []
    file_counter = 1
    total_messages = 0
    
    try:
        print(f"Starting to consume from topic '{topic_name}' and save to Parquet...")
        
        for message in consumer:
            if message.value:
                # Parse JSON transaction
                transaction = json.loads(message.value)
                transactions.append(transaction)
                total_messages += 1
                
                # Save batch when we reach batch_size
                if len(transactions) >= batch_size:
                    save_parquet_batch(transactions, output_directory, topic_name, file_counter)
                    transactions = []  # Reset for next batch
                    file_counter += 1
                    
                if total_messages % 100 == 0:
                    print(f"Processed {total_messages} messages...")
                    
    except KeyboardInterrupt:
        print(f"\nStopping... Processed {total_messages} messages")
    finally:
        consumer.close()
        
        # Save remaining transactions
        if transactions:
            save_parquet_batch(transactions, output_directory, topic_name, file_counter)
            
        print(f"Total messages saved: {total_messages}")

def save_parquet_batch(transactions, output_directory, topic_name, file_counter):
    """Save a batch of transactions to Parquet file"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_directory}/{topic_name}/{topic_name}_batch_{file_counter}_{timestamp}.parquet"
    
    # Convert to DataFrame
    df = pd.DataFrame(transactions)
    
    # Convert timestamp columns to datetime
    if 'TIMESTAMP' in df.columns:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    if 'TIMESTAMP_OF_RECEPTION_LOG' in df.columns:
        df['TIMESTAMP_OF_RECEPTION_LOG'] = pd.to_datetime(df['TIMESTAMP_OF_RECEPTION_LOG'])
    
    # Save to Parquet
    df.to_parquet(filename, index=False)
    print(f"Saved {len(transactions)} transactions to: {filename}")

# Usage
if __name__ == "__main__":
    consume_kafka_to_parquet(
        topic_name="TRANSACTIONS_CLEANED",
        bootstrap_servers=['localhost:9092'],
        output_directory="./data_lake",
        batch_size=1000
    )