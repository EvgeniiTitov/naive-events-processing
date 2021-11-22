import time

from app import ElasticApp


def main() -> int:
    app = ElasticApp()
    app.start()
    try:
        for i in range(60):
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    app.stop()

    return 0


if __name__ == "__main__":
    main()
