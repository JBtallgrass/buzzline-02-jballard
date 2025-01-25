import os
import sys
import json
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from loguru import logger
sys.path.append("c:/Users/balla/Projects/buzzline-02-jballard")
from utils.utils_consumer import create_kafka_consumer

load_dotenv()

class WeatherAnalytics:
    def __init__(self):
        self.city_data = defaultdict(lambda: {
            'temps': [],
            'max_temp': float('-inf'),
            'min_temp': float('inf'),
            'extreme_events': [],
            'last_updated': None
        })
        self.messages_processed = 0

    def update(self, city, temp, condition):
        data = self.city_data[city]
        data['temps'].append(temp)
        data['max_temp'] = max(data['max_temp'], temp)
        data['min_temp'] = min(data['min_temp'], temp)
        data['last_updated'] = datetime.now()
        self.messages_processed += 1
        
        if temp > 30 or temp < 0 or condition in ['Stormy', 'Snowy']:
            data['extreme_events'].append({
                'temp': temp,
                'condition': condition,
                'timestamp': datetime.now()
            })

    def get_city_report(self, city):
        data = self.city_data[city]
        if not data['temps']:
            return None
            
        return {
            'avg_temp': round(sum(data['temps'])/len(data['temps']), 1),
            'temp_range': (round(data['min_temp'], 1), round(data['max_temp'], 1)),
            'extreme_count': len(data['extreme_events']),
            'last_extreme': data['extreme_events'][-1] if data['extreme_events'] else None
        }

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "buzzline")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "weather_analytics")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def process_message(message: str, analytics: WeatherAnalytics) -> None:
    try:
        data = json.loads(message)
        city = data['location']
        temp = data['temperature_celsius']
        condition = data['condition']
        
        analytics.update(city, temp, condition)
        
        if analytics.messages_processed % 10 == 0:
            logger.info("\n=== Weather Analysis Report ===")
            for city in analytics.city_data:
                report = analytics.get_city_report(city)
                if report:
                    logger.info(f"\n{city}:")
                    logger.info(f"Temperature Range: {report['temp_range'][0]}°C to {report['temp_range'][1]}°C")
                    logger.info(f"Average Temperature: {report['avg_temp']}°C")
                    logger.info(f"Extreme Events: {report['extreme_count']}")
                    if report['last_extreme']:
                        logger.info(f"Last Extreme Event: {report['last_extreme']['condition']} "
                                  f"at {report['last_extreme']['temp']}°C")
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
            process_message(message.value, analytics)
    except KeyboardInterrupt:
        logger.warning("Weather analytics interrupted by user.")
        