from google.cloud import pubsub_v1

from config import Config


samples = [
    [5.1, 3.5, 1.4, 0.2],
    [4.9, 3.0, 1.4, 0.2],
    [7.0, 3.2, 4.7, 1.4],
    [6.4, 3.2, 4.5, 1.5],
    [6.3, 3.3, 6.0, 2.5],
    [5.8, 3.3, 6.0, 2.5],
]


def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        Config.PROJECT_ID, Config.CONSUME_TOPIC_ID
    )
    print("Initialized")

    for i in range(5):
        message = {"crn": i, "features": samples[i % len(samples)]}
        message_encoded = str(message).encode("utf-8")
        future = publisher.publish(topic_path, message_encoded)
        print("Published message:", message_encoded)
        print(future.result())


if __name__ == "__main__":
    main()
