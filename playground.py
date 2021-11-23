from multiprocessing import Process
import os


class SampleWorker(Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("My pid:", os.getpid())

    def run(self) -> None:
        print("My pid:", os.getpid())


def main():
    print("Main process pid:", os.getpid())
    worker = SampleWorker()
    worker.start()
    worker.join()


if __name__ == "__main__":
    main()
