# buzzline-02-jballard

# Weather Analytics Kafka System

## Custom Producer and Consumer for Buzzline

Welcome to my GitHub repository! I'm Jason Ballard, an aspiring data scientist and instructional systems specialist passionate about exploring the intersections of data, technology, and learning. You can learn more about my work and projects on my GitHub profile: [JBtallgrass](https://github.com/JBtallgrass).

This project is part of a module on streaming analytics designed to deepen understanding of data-in-motion concepts, tools, and workflows. It demonstrates the development of a custom producer and consumer for generating and analyzing real-time messages related to outdoor activities. Through this project, I explore how real-time data flows can be leveraged for actionable insights.

## Project Purpose and Learning Objectives

### Purpose:
This project aims to create a foundational framework for understanding and applying streaming analytics principles using Python. By simulating a producer-consumer model, it highlights the fundamentals of data-in-motion, enabling real-time decision-making and analytics.

### Learning Objectives:

#### Conceptual Understanding:
- Describe the core concepts of streaming analytics, differentiating between data-in-motion and data-at-rest.

#### Tool Mastery:
- Select and configure essential tools, including Python, GitHub, Git, and VS Code, for streaming analytics workflows.

#### Practical Application:
- Apply Python scripting to create and manage data-in-motion workflows involving producers and consumers.

#### Environment Setup:
- Plan and execute foundational setup tasks to enable streaming analytics development, including environment configuration and tool integration.

## Overview

- **`kafka_producer_jballard.py`:** Simulates weather station data and streams it to a Kafka topic.
- **`kafka_consumer_jballard.py`:** Consumes the weather data, processes it to compute analytics, and logs results including extreme weather conditions.

## Features

### Producer (`kafka_producer_jballard.py`)
- Simulates weather data for multiple locations, including temperature, condition, humidity, and timestamp.
- Sends data as structured JSON messages to the Kafka topic.
- Includes heat and freeze alerts based on temperature thresholds.
- Configurable message interval using `.env` variables.

### Consumer (`kafka_consumer_jballard.py`)
- Consumes weather data messages from the Kafka topic.
- Processes messages to:
  - Calculate statistics (average, max, min temperature, and number of readings).
  - Identify and log extreme weather conditions (e.g., storms, snow).
- Logs periodic updates and recent extreme weather events.

## Requirements

- Python 3.11
- Apache Kafka
- Kafka Python client libraries
- Required Python packages (see `requirements.txt`):
  - `kafka-python`
  - `python-dotenv`
  - `statistics`
  - `json`
  - `datetime`

## Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the project root with the following content:

```env
KAFKA_TOPIC=weather_stream
KAFKA_CONSUMER_GROUP_ID_JSON=weather_analytics
MESSAGE_INTERVAL_SECONDS=2
```
- **KAFKA_TOPIC:** Kafka topic to send/receive messages.
- **KAFKA_CONSUMER_GROUP_ID_JSON:** Consumer group ID for the consumer.
- **MESSAGE_INTERVAL_SECONDS:** Interval in seconds between messages sent by the producer.

### 4. Start Kafka Server
Ensure Apache Kafka is running and accessible. You can download Kafka [here](https://kafka.apache.org/downloads).

### 5. Run the Scripts

#### Run the Producer
```bash
python kafka_producer_jballard.py
```

#### Run the Consumer
```bash
python kafka_consumer_jballard.py
```

## How It Works

### Producer Workflow
1. Generates weather data using random values for temperature, humidity, and conditions.
2. Sends the data as a JSON message to the specified Kafka topic.
3. Logs messages and sends alerts for extreme temperatures.

### Consumer Workflow
1. Consumes messages from the Kafka topic.
2. Processes the data to compute analytics per location.
3. Logs periodic updates and identifies recent extreme conditions.

## Example Logs

### Producer Output:
```
INFO:Kafka topic: weather_stream
INFO:Weather station online - Starting data transmission...
INFO:Sent: New York: 24.5°C, Sunny
INFO:Sent: Tokyo: 32.1°C, Stormy 🔥 HEAT ALERT!
```

### Consumer Output:
```
INFO:Kafka topic: weather_stream
INFO:=== Weather Analytics Update ===
INFO:New York: Avg 24.5°C, Range: 22.0°C to 26.0°C
INFO:Recent Extreme Conditions:
INFO:Tokyo: Stormy at 32.1°C
```

## Customization

- Modify `generate_weather_data()` in `kafka_producer_jballard.py` to add or change locations and weather conditions.
- Adjust thresholds for alerts in `process_message()` in `kafka_consumer_jballard.py`.

## Troubleshooting

- Ensure Kafka is running and the topic exists before starting the producer or consumer.
- Use the provided utilities to create a Kafka topic if it doesn’t exist.
- Verify environment variables are correctly set in the `.env` file.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Author
Jason Ballard


