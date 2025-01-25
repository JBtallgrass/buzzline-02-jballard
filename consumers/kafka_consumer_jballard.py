import os
import json
from datetime import datetime
import statistics
from collections import defaultdict
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

load_dotenv()

class WeatherAnalytics:
    def __init__(self):
        self.location_temps = defaultdict(list)
        self.extreme_conditions = []
        self.messages_processed = 0
        
    def update_stats(self, location: str, temp: float):
        self.location_temps[location].append(temp)
        
    def get_location_stats(self, location: str) -> dict:
        temps = self.location_temps[location]
        if not temps:
            return None
        return {
            "avg_temp": round(statistics.mean(temps), 1),
            "max_temp": max(temps),
            "min_temp": min(temps),
            "readings": len(temps)
        }

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "weather_stream")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "weather_analytics")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def process_message(message: str, analytics: WeatherAnalytics) -> None:
    try:
        data = json.loads(message)
        location = data["location"]
        temp = data["temperature_celsius"]
        condition = data["condition"]
        
        analytics.update_stats(location, temp)
        analytics.messages_processed += 1
        
        if temp > 30 or temp < 0 or condition in ["Stormy", "Snowy"]:
            analytics.extreme_conditions.append({
                "location": location,
                "condition": condition,
                "temperature": temp,
                "timestamp": data["timestamp"]
            })
        
        if analytics.messages_processed % 10 == 0:
            logger.info("\n=== Weather Analytics Update ===")
            for loc in analytics.location_temps.keys():
                stats = analytics.get_location_stats(loc)
                logger.info(f"{loc}: Avg {stats['avg_temp']}°C, "
                          f"Range: {stats['min_temp']}°C to {stats['max_temp']}°C")
            
            if analytics.extreme_conditions:
                logger.info("\nRecent Extreme Conditions:")
                for event in analytics.extreme_conditions[-3:]:
                    logger.info(f"{event['location']}: {event['condition']} "
                              f"at {event['temperature']}°C")
            
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message as JSON: {e}")
    except KeyError as e:
        logger.error(f"Missing required field in message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main() -> None:
    logger.info("Starting weather analytics system...")
    
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    analytics = WeatherAnalytics()
    
    consumer = create_kafka_consumer(topic, group_id)
    
    try:
        for message in consumer:
            message_str = message.value.decode('utf-8')
            process_message(message_str, analytics)
    except KeyboardInterrupt:
        logger.warning("Weather analytics interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info("Weather analytics system offline.")

if __name__ == "__main__":
    main()