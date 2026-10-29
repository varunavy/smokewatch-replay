"""Create the three Kafka topics used by the wildfire-smoke pipeline.

Usage:
    python src/kafka/create_topics.py [--bootstrap localhost:9092]
"""
import argparse

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import BOOTSTRAP_SERVERS, NUM_PARTITIONS, REPLICATION_FACTOR, TOPICS


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap)
    for source, topic in TOPICS.items():
        try:
            admin.create_topics(
                [NewTopic(topic, num_partitions=NUM_PARTITIONS,
                          replication_factor=REPLICATION_FACTOR)]
            )
            print(f"created  {topic}  (source={source})")
        except TopicAlreadyExistsError:
            print(f"exists   {topic}  (source={source})")
    admin.close()


if __name__ == "__main__":
    main()
