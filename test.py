import time

from app.message_consumer.pubsub import PubSubMessageConsumer
from app.result_publisher.pubsub import PubSubMessagePublisher


if __name__ == "__main__":
    # consumer = PubSubMessageConsumer()
    #
    # try:
    #     while True:
    #         message = consumer.get_messages()
    #         print("Got message:", message)
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     print("Done")

    test = [
        [0, [5.1, 3.5, 1.4, 0.2], "setosa"],
        [1, [4.9, 3.0, 1.4, 0.2], "setosa"],
    ]
    publisher = PubSubMessagePublisher()
    publisher.publish_result(test)

    print("Done")
