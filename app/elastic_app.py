"""
Naive elastic app

Instead of just having multiple app instances running all the time (not very
efficient if the load is not so high), create a system that spawns (creates) new
instances of the app when the load is high and kills running apps when the load
is low.

New object Puller - pull as many messages from PubSub as quickly as possible and
puts them in a big ass job queue (potentially multiple instances of the puller
populating the same job queue)

New object Distributor - gets the job queue and then round robins tasks across N
workers.
    - It starts with N workers (put them in different processes) connected to the
    Distributor via pipes.
    - It starts sending messages from the job queue to each worker following the
    Round Robin approach
    - It monitors worker pipes, if they are full (.put_no_wait() raises an Exception),
    then the Distributor spawns a new worker and adds it to the pull of currently
    running workers. If pipes are empty (periodically check their sizes), then put a
    kill message to one of the workers and then join it, so the process dies
"""

# DUMMY EXAMPLE
# TODO: How to do something once in N amount of time in the event loop?

import time
import multiprocessing
import threading
import typing as t
import queue
import random

from app.helpers import LoggerMixin, get_pid_number


fake_userdefined_object = t.Any


class Worker(multiprocessing.Process, LoggerMixin):
    """
    Fake worker running in a separate process connected by the pipe to the
    Distributor sitting in the main process. The worker gets jobs to process
    through the pipe.
    """

    def __init__(
        self,
        job_queue: multiprocessing.Queue,
        message_processor: fake_userdefined_object,
        result_publisher: fake_userdefined_object,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._job_queue = job_queue
        self._message_processor = message_processor
        self._result_publisher = result_publisher
        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - Worker inited")

    @property
    def job_queue(self) -> multiprocessing.Queue:
        return self._job_queue

    @property
    def my_capacity(self) -> t.Tuple[int, int]:
        return self._job_queue.qsize(), self._job_queue._maxsize

    def run(self) -> None:
        while True:
            task: t.Any = self._job_queue.get()
            if "STOP" in task:
                break
            time.sleep(2)  # Long lasting task processing
            print(f"PID: {self._pid} - processed task {task}")

        self.logger.info(f"PID: {self._pid} - Worker stopped")


class MessagePuller(threading.Thread, LoggerMixin):
    """
    Fake message generator running in a thread in the main process whose main
    task is to pull messages (create fake ones) to get processed by the worker.
    The MessagePuller places messages into the queue_out connected to the
    Distributor
    """

    def __init__(
        self,
        queue_in: queue.Queue,
        job_queue_out: queue.Queue,
        message_puller: fake_userdefined_object,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._job_queue_out = job_queue_out
        self._message_puller = message_puller
        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - MessagePuller inited")

    def run(self) -> None:
        job_counter = 0
        while True:
            try:
                task = self._queue_in.get_nowait()
            except queue.Empty:
                pass
            else:
                if "STOP" in task:
                    break

            new_job = f"Task: {job_counter}"
            job_counter += 1
            self._job_queue_out.put(new_job)

        self.logger.info(f"PID: {self._pid} - MessagePuller stopped")


class JobDistributor(threading.Thread, LoggerMixin):
    """
    Fake job (message) distributor running in a thread in the main process
    whose main task is to:
    1. Distribute messages from the job queue
    2. Handle the load by monitoring job pipes to the workers and
       creating/killing workers depending on the number of jobs in their pipes
       i.e the pipe is empty? -> kill the worker, the pipes of the existing
       worker are full? -> spawn a new process and add to the pool
    """

    def __init__(
        self,
        queue_in: queue.Queue,
        job_queue_in: queue.Queue,
        n_initial_workers: int,
        worker_queue_size: int,
        job_distribution_batch_size: int,
        *args,
        **kwargs,
    ) -> None:
        super(JobDistributor, self).__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._job_queue = job_queue_in
        self._n_workers = n_initial_workers
        self._worker_queue_size = worker_queue_size
        self._job_batch_size = job_distribution_batch_size
        self._running_workers: t.List[Worker] = []
        self._stopping_workers: t.List[Worker] = []
        self._pid = get_pid_number()
        self._identity = f"PID: {self._pid} - JobDistributor"
        self.logger.info(f"{self._identity} inited")

    def run(self) -> None:
        """
        Main event loop of the Distributor
        """
        self._crete_min_number_of_workers()
        self.logger.info(f"{self._identity} started {self._n_workers} workers")
        while True:
            stop = self._check_if_time_to_stop()
            if stop:
                self._stop_running_workers()
                break

            # TODO: Doing this each loop is expensive
            self._scale_workers()

            self._distribute_tasks_across_workers()
        self.logger.info(f"{self._identity} stopped")

    def _scale_workers(self) -> None:
        """
        Check if we need more workers or too many and we need to kill one

        if new required:
            - Create a worker
            - Add it to the pool

        If too many:
            - Send the stop message to this worker
            - Make sure it doesnt get new tasks, pop from the pool?
            - Make sure it stopped successfully?
        """
        direction = self._determine_scaling_direction()
        if direction == "UP":
            self._append_new_worker_to_running_pool()
            return
        elif direction == "DOWN":
            self._remove_worker_from_running_pool()
        self._join_stopping_workers()

    def _distribute_tasks_across_workers(self) -> None:
        raise NotImplementedError

    def _remove_worker_from_running_pool(self) -> None:
        """
        IT IS ASSUMED for simplicity sake to remove a random worker, not the
        one with the least tasks in the queue
        """
        worker_to_stop = self._running_workers.pop(
            random.randint(0, len(self._running_workers))
        )
        worker_to_stop.job_queue.put("STOP")
        self._stopping_workers.append(worker_to_stop)

    def _append_new_worker_to_running_pool(self) -> None:
        worker = self._create_new_worker()
        self._running_workers.append(worker)

    def _check_if_time_to_stop(self) -> bool:
        try:
            task = self._queue_in.get_nowait()
        except queue.Empty:
            return False
        else:
            if "STOP" in task:
                return True
            else:
                self.logger.warning(
                    f"{self._identity} got unknown command {task}, ignored"
                )
                return False

    def _join_stopping_workers(self) -> None:
        if not len(self._stopping_workers):
            return
        self._stopping_workers = [
            worker
            for worker in self._running_workers
            if not self._join_worker_within_timeout(worker)
        ]

    def _join_worker_within_timeout(
        self, worker: Worker, timeout: float = 0.1
    ) -> bool:
        try:
            worker.join(timeout)
        except TimeoutError:
            return False
        else:
            return True

    def _determine_scaling_direction(self) -> str:
        """
        IT IS ASSUMED that a new worker is required when there is not enough
        space to distribute a batch of messages across currently
        running workers.
        Kill a worker when too many slots in the worker queues (batch x 2)
        Else do not change the number of workers
        """
        currently_available_slots = 0
        for worker in self._running_workers:
            jobs_in_queue, queue_size = worker.my_capacity
            currently_available_slots += queue_size - jobs_in_queue

        if currently_available_slots <= self._job_batch_size:
            return "UP"
        elif currently_available_slots > self._job_batch_size * 2:
            return "DOWN"
        else:
            return "KEEP"

    def _stop_running_workers(self) -> None:
        self._signal_workers_to_stop()
        self.logger.info(f"{self._identity} signalled workers to stop")
        self._join_workers()
        self.logger.info(f"{self._identity} joined the workers")

    def _join_workers(self) -> None:
        while self._running_workers:
            worker = self._running_workers.pop()
            worker.join()  # Could hang indefinitely
            # worker.job_queue.close()

            # TODO: Could be done smarter - randomize worker to join each time
            # worker = self._running_workers[0]
            # try:
            #     worker.join(timeout=1.0)
            # except TimeoutError:
            #     self.logger.info(
            #         f"{self._identity} failed to join worker within "
            #         f"timeout, skipping"
            #     )
            #     continue
            # else:
            #     self._running_workers.pop(0)

    def _signal_workers_to_stop(self) -> None:
        for worker in self._running_workers:
            worker.job_queue.put("STOP")

    def _crete_min_number_of_workers(self) -> None:
        # Create min number of workers
        workers = []
        for i in range(self._n_workers):
            worker = self._create_new_worker()
            workers.append(worker)
        # Start the workers
        for worker in workers:
            worker.start()
            self._running_workers.append(worker)

    def _create_new_worker(self) -> Worker:
        worker_job_queue = multiprocessing.Queue(self._worker_queue_size)
        worker = Worker(
            job_queue=worker_job_queue,
            message_processor="fake message processor",
            result_publisher="fake result publisher",
        )
        return worker
