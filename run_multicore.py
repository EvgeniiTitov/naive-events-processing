import time
import multiprocessing

from app import App
from app.helpers import get_pid_number


CPU_COUNT = multiprocessing.cpu_count()
TEST_TIME = 60 * 5


def run_application(test_time: int) -> None:
    app = App()
    app.start()
    queue_sizes = []
    for i in range(test_time):
        time.sleep(1)
        if i % 10 == 0:
            queue_sizes.append(app.report_queue_sizes())
    app.stop()
    print(f"\n\nProcess's {get_pid_number()} queue sizes {queue_sizes}")


def main() -> int:
    processes = []
    for i in range(CPU_COUNT):
        process = multiprocessing.Process(
            target=lambda: run_application(TEST_TIME)
        )
        process.start()
        processes.append(process)
    print(f"Spawned {CPU_COUNT} processes")

    for process in processes:
        process.join()

    print("Done - all processes finished")
    return 0


if __name__ == "__main__":
    main()
