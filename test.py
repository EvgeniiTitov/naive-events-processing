import time

from app.pubsub_consumer.consumer import MessageConsumer


if __name__ == '__main__':
    consumer = MessageConsumer()

    try:
        while True:
            message = consumer.get_messages()
            print("Got message:", message)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Done")
