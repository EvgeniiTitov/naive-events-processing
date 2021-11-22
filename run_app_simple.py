import time

from app import App


TEST_TIME = 60 * 1


def main() -> int:
    app = App()
    app.start()
    queue_sizes = []
    try:
        for i in range(TEST_TIME):
            time.sleep(1)
            if i % 10 == 0:
                queue_sizes.append(app.report_queue_sizes())
    except KeyboardInterrupt:
        pass
    app.stop()

    print(
        f"\n\nProcessed {app.processed_messages} messages in "
        f"{TEST_TIME} seconds"
    )
    # print("QUEUE SIZES:", queue_sizes)
    return 0


if __name__ == "__main__":
    main()
