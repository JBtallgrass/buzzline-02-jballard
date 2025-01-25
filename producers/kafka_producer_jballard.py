import os
import sys
import time
import random
from datetime import datetime
from dataclasses import dataclass
import json
from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

load_dotenv()

@dataclass
class WeatherData:
    location: str
    temperature: float
    condition: str
    humidity: int
    timestamp: str

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "weather_stream")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def generate_weather_data() -> WeatherData:
    locations = ["New York", "London", "Tokyo", "Sydney", "Paris"]
    conditions = ["Sunny", "Cloudy", "Rainy", "Stormy", "Snowy", "Windy"]
    
    location = random.choice(locations)
    base_temp = {
        "New York": 20, "London": 15, "Tokyo": 25,
        "Sydney": 28, "Paris": 18
    }[location]
    
    temperature = round(base_temp + random.uniform(-5, 5), 1)
    condition = random.choice(conditions)
    humidity = random.randint(30, 90)
    timestamp = datetime.now().isoformat()
    
    return WeatherData(location, temperature, condition, humidity, timestamp)

def generate_messages(producer, topic, interval_secs):
    try:
        while True:
            weather_data = generate_weather_data()
            message = {
                "location": weather_data.location,
                "temperature_celsius": weather_data.temperature,
                "condition": weather_data.condition,
                "humidity": weather_data.humidity,
                "timestamp": weather_data.timestamp
            }
            
            json_message = json.dumps(message)
            producer.send(topic, value=json_message.encode('utf-8'))
            
            alert = ""
            if weather_data.temperature > 30:
                alert = "🔥 HEAT ALERT!"
            elif weather_data.temperature < 0:
                alert = "❄️ FREEZE ALERT!"
            
            log_msg = (f"{weather_data.location}: {weather_data.temperature}°C, "
                      f"{weather_data.condition} {alert}")
            logger.info(f"Sent: {log_msg}")
            
            time.sleep(interval_secs)
            
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Weather station offline - Kafka producer closed.")

def main():
    logger.info("Starting weather station simulation...")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Weather stream topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info("Weather station online - Starting data transmission...")
    generate_messages(producer, topic, interval_secs)

if __name__ == "__main__":
    main()