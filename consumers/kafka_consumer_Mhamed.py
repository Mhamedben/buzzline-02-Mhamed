import os
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables from the .env file
load_dotenv()

# Function to get Kafka topic from environment or use default
def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

# Function to get Kafka consumer group ID from environment or use default
def get_kafka_consumer_group_id() -> int:
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID_JSON", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

# Function to process each message and perform real-time analytics
def process_message(message: str) -> None:
    """
    Process a single message.

    If the message contains the word "ERROR", an alert will be generated.
    """
    logger.info(f"Processing message: {message}")

    # Real-time analytics: Check for the presence of the word "ERROR"
    if "ERROR" in message:
        # If the word "ERROR" is found, log an alert
        logger.warning(f"ALERT: ERROR detected in message: {message}")
    else:
        # Otherwise, just log that the message was processed successfully
        logger.info(f"Message processed successfully: {message}")

# Main function to consume messages from Kafka topic
def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes messages from the Kafka topic.
    """
    logger.info("START consumer.")
    
    # Fetch topic and group id from .env file
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create Kafka consumer using the utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

# Entry point for script execution
if __name__ == "__main__":
    main()

