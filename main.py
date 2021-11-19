import time

from app import App


TEST_TIME = 60 * 5


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

    print("\n\nQUEUE SIZES:", queue_sizes)
    return 0


if __name__ == "__main__":
    main()
