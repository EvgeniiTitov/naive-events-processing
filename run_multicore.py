import time
import multiprocessing
from functools import partial

from app import App


CPU_COUNT = 4  # multiprocessing.cpu_count()
TEST_TIME = 60 * 1


def run_application(index: int, test_time: int) -> int:
    print("Starting application:", index)
    app = App()
    app.start()
    # queue_sizes = []
    for i in range(test_time):
        time.sleep(1)
        if i % 10 == 0:
            # queue_sizes.append(app.report_queue_sizes())
            pass
    app.stop()
    return app.processed_messages


def main() -> int:
    with multiprocessing.Pool(CPU_COUNT) as pool:
        results = pool.map(
            func=partial(run_application, test_time=TEST_TIME),
            iterable=range(CPU_COUNT),
        )

    print(
        f"{CPU_COUNT} apps processed {sum(results)} messages "
        f"in {TEST_TIME} seconds"
    )
    return 0


if __name__ == "__main__":
    main()
