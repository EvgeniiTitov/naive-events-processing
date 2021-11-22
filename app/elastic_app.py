"""
Naive elastic app

Instead of just having multiple app instances running all the time (not very
efficient if the load is not so high), create a system that spawns (creates)
new instances of the app (workers) when the load is high and kills running
apps when the load is low.

New object Puller - pulls as many messages from PubSub as quickly as possible
and puts them in a big ass job queue (potentially multiple instances of the
puller populating the same job queue)

New object Distributor - gets the job queue and then round robins tasks across
N workers.
    - It starts with N workers (put them in different processes) connected to
    the Distributor via queues.
    - It starts sending messages from the job queue to each worker following
    the Round Robin approach
    - It monitors worker queues, if they are full (.put_no_wait() raises an
    Exception), then the Distributor spawns a new worker and adds it to the
    pull of currently running workers. If queues are empty (periodically check
    their sizes), then put a kill message to one of the workers and then
    join it, so the process dies

No mutexes used, each worker gets a dedicated Queue, no sharing of resources


Issues:
1. Picking / unpickling heavy messages is expensive (IPC python issue) ->
   Apache Arrow / Ray or similar should fix the issue.
2. Scaling is possible within one machine only, cannot utilize a cluster ->
   RPCs?
3. Single Puller / Single job queue
"""

# DUMMY EXAMPLE - probably very inefficient
# TODO: Currently messages in the queue from Puller to Distributor get lost if
#       app is stopped
# TODO: Oscillations up down

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
        self._pid = None
        self._iam = f"PID: {self._pid} - Worker"

    @property
    def job_queue(self) -> multiprocessing.Queue:
        return self._job_queue

    @property
    def my_capacity(self) -> t.Tuple[int, int]:
        return self._job_queue.qsize(), self._job_queue._maxsize

    def run(self) -> None:
        self._pid = get_pid_number()  # Child process PID
        self.logger.info(f"{self._iam} - started")
        queue_capacity = self._job_queue._maxsize
        while True:
            task: t.Any = self._job_queue.get()
            if "STOP" in task:
                break

            # TODO: Proper task processor and result publisher implementation
            # Long lasting task processing using the passed objects:
            # message processor and result publisher
            time.sleep(2)
            self.logger.info(
                f"{self._iam} - processed task {task}; "
                f"My queue size: {self.job_queue.qsize()}/{queue_capacity}"
            )
        self.logger.info(f"{self._iam} - stopped")


class MessagePuller(threading.Thread, LoggerMixin):
    """
    Fake message generator running in a thread in the main process whose main
    task is to pull messages (create fake ones) to get processed by the worker.
    The MessagePuller places messages into the queue_out connected to the
    Distributor
    """

    SLEEP_MAX = 100
    SLEEP_MIN = 10
    DELTA = 10

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

        self._sleep_time = 50

        self._pid = get_pid_number()
        self._iam = f"PID: {self._pid} - MessagePuller"
        self.logger.info(f"{self._iam} inited")

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
                elif "DECREASE_LOAD" in task:
                    if self._sleep_time + self.DELTA <= self.SLEEP_MAX:
                        self._sleep_time += self.DELTA
                        self.logger.info(f"{self._iam} - load decreased")
                    else:
                        self.logger.info(f"{self._iam} - min load reached")
                elif "INCREASE_LOAD" in task:
                    if self._sleep_time - self.DELTA >= self.SLEEP_MIN:
                        self._sleep_time -= self.DELTA
                        self.logger.info(f"{self._iam} - load increased")
                    else:
                        self.logger.info(f"{self._iam} - max load reached")
                else:
                    self.logger.info(f"{self._iam} - got unknown task :(")

            # TODO: Proper message_puller implementation
            # Fake message creation - in reality it use the message_puller
            # object passed by the user, which extracts messages from somewhere
            # say a PubSub topic
            new_job = f"Task: {job_counter}"
            job_counter += 1
            try:
                self._job_queue_out.put(new_job, timeout=1.0)
            except Exception:
                pass

            time.sleep(self._sleep_time / 100)

        self.logger.info(f"{self._iam} stopped")


class JobDistributor(threading.Thread, LoggerMixin):
    """
    Fake job (message) distributor running in a thread in the main process
    whose main task is to:
    1. Distribute messages from the job queue across workers
    2. Handle the load by monitoring job pipes to the workers and
       creating/killing workers depending on the number of jobs in their pipes
    """

    def __init__(
        self,
        queue_in: queue.Queue,
        job_queue_in: queue.Queue,
        n_initial_workers: int,
        worker_queue_size: int,
        job_distribution_batch_size: int,
        max_workers: int,
        *args,
        **kwargs,
    ) -> None:
        super(JobDistributor, self).__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._job_queue = job_queue_in
        self._min_n_workers = n_initial_workers
        self._worker_queue_size = worker_queue_size
        self._job_batch_size = job_distribution_batch_size
        self._max_n_workers = max_workers

        self._running_workers: t.List[Worker] = []
        self._stopping_workers: t.List[Worker] = []

        self._pid = get_pid_number()
        self._iam = f"PID: {self._pid} - JobDistributor"
        self.logger.info(f"{self._iam} - inited")

    def run(self) -> None:
        """
        Main event loop of the Distributor
        """
        self._crete_min_number_of_workers()
        self.logger.info(
            f"{self._iam} - started {self._min_n_workers} workers"
        )
        while True:
            # Check if its time to stop the distributor
            stop = self._check_if_time_to_stop()
            if stop:
                # Before the distributor can be stopped, stop all workers
                self._stop_all_workers()
                break

            # Ensure the appropriate number of workers are running depending
            # on the load
            self._manage_workers()

            # Receive a batch of jobs and distribute across the running workers
            self._distribute_jobs_batch_across_workers()

        self.logger.info(f"{self._iam} - stopped")

    def _manage_workers(self) -> None:
        direction = self._determine_scaling_direction()
        n_running_workers = len(self._running_workers)

        if direction == "UP" and n_running_workers < self._max_n_workers:
            self._append_new_worker_to_running_pool()
        elif direction == "DOWN" and n_running_workers > self._min_n_workers:
            self._remove_worker_from_running_pool()
        else:
            self.logger.info(
                f"{self._iam} - Currently running workers: "
                f"{len(self._running_workers)}"
            )
        if len(self._stopping_workers):
            self._try_joining_stopping_workers_within_timeout()

    def _distribute_jobs_batch_across_workers(self) -> None:
        # Get a batch of jobs from the job queue coming from the Puller
        jobs_batch = self._accumulage_jobs_batch()
        if not len(jobs_batch):
            self.logger.info(
                f"{self._iam} - failed to accumulate a batch of jobs, "
                f"the job queue is empty"
            )
            return

        # Distribute the jobs across the running workers (Round Robin)
        while jobs_batch:
            job = jobs_batch.pop()
            # TODO: Consider shuffling workers before distributing the tasks,
            #       it appears the first ones might be potentially busier?
            for worker in self._get_next_worker_round_robin():
                # If worker queue is full, try the next worker
                try:
                    worker.job_queue.put_nowait(job)
                except Exception:
                    continue
                else:
                    break
        self.logger.info(f"{self._iam} - a batch of jobs distributed")

    def _get_next_worker_round_robin(self) -> t.Iterator[Worker]:
        while True:
            for worker in self._running_workers:
                yield worker

    def _accumulage_jobs_batch(self) -> t.List[t.Any]:
        batch = []
        for i in range(self._job_batch_size):
            try:
                job = self._job_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            batch.append(job)
        return batch

    def _remove_worker_from_running_pool(self) -> None:
        """
        IT IS ASSUMED for simplicity sake to remove a random worker, not the
        one with the least tasks in the queue
        """
        # TODO: How to quickly find a worker with the fewest jobs in its queue?
        worker_to_stop = self._running_workers.pop(
            random.randrange(0, len(self._running_workers))
        )
        worker_to_stop.job_queue.put("STOP")
        self._stopping_workers.append(worker_to_stop)
        self.logger.info(
            f"{self._iam} - scaling down started, signalled worker to stop"
        )

    def _append_new_worker_to_running_pool(self) -> None:
        worker = self._create_new_worker()
        worker.start()
        self._running_workers.append(worker)
        self.logger.info(f"{self._iam} - scaled up, added new worker")

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
                    f"{self._iam} - got unknown command {task}, ignored"
                )
                return False

    def _try_joining_stopping_workers_within_timeout(self) -> None:
        failed_to_join_within_timeout = []
        for worker in self._stopping_workers:
            joined = self._join_worker_within_timeout(worker)
            if joined:
                self.logger.info(
                    f"{self._iam} - scaling down complete, worker killed"
                )
            else:
                failed_to_join_within_timeout.append(worker)
        self._stopping_workers = failed_to_join_within_timeout

    def _join_worker_within_timeout(
        self, worker: Worker, timeout: float = 0.1
    ) -> bool:
        try:
            worker.join(timeout)
        except TimeoutError:
            return False
        else:
            return True

    def _join_stopping_workers(self) -> None:
        # TODO: This is bad and might hang indefinitely
        while self._stopping_workers:
            stopping_worker = self._stopping_workers.pop()
            stopping_worker.join()
            # stopping_worker.job_queue.close()

    def _determine_scaling_direction(self) -> str:
        """
        IT IS ASSUMED that a new worker is required when there is not enough
        space to distribute a batch of messages across currently
        running workers.
        Kill a worker when too many slots in the worker queues (batch x 2)
        Else do not change the number of workers
        """
        # TODO: Double check your scaling logic based on slots makes sense
        #       IT DOESNT! Spikes up down up down
        currently_available_queue_slots = 0
        for worker in self._running_workers:
            jobs_in_queue, queue_size = worker.my_capacity
            currently_available_queue_slots += queue_size - jobs_in_queue
        self.logger.debug(
            f"{self._iam} - available qeueue slots:"
            f" {currently_available_queue_slots}"
        )
        if currently_available_queue_slots <= self._job_batch_size:
            return "UP"
        elif currently_available_queue_slots > self._job_batch_size * 2:
            return "DOWN"
        else:
            return "KEEP"

    def _stop_all_workers(self) -> None:
        self._signal_running_workers_to_stop()
        self.logger.info(f"{self._iam} - signalled running workers to stop")
        self._join_stopping_workers()
        self.logger.info(f"{self._iam} - joined the workers")
        if len(self._running_workers) or len(self._stopping_workers):
            raise Exception(f"{self._iam} failed to stop all workers!")

    def _signal_running_workers_to_stop(self) -> None:
        while self._running_workers:
            running_worker = self._running_workers.pop()
            running_worker.job_queue.put("STOP")
            self._stopping_workers.append(running_worker)

    def _crete_min_number_of_workers(self) -> None:
        # Create min number of workers
        workers = []
        for i in range(self._min_n_workers):
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


class ElasticApp(LoggerMixin):

    NUMBER_OF_WORKERS = 2
    MAX_NUMBER_OF_WORKERS = 8
    JOB_QUEUE_SIZE = 100
    WORKER_QUEUE_SIZE = 10
    JOB_BATCH_SIZE = 10

    def __init__(self):
        self._pid = get_pid_number()
        self._identity = f"PID: {self._pid} - ElasticApp"

        self._puller_status_queue = queue.Queue()
        self._distributor_status_queue = queue.Queue()
        self._job_queue = queue.Queue(ElasticApp.JOB_QUEUE_SIZE)
        self.logger.info(f"{self._identity} - Queues inited")

        self._threads = []

        self._message_puller_thread = MessagePuller(
            queue_in=self._puller_status_queue,
            job_queue_out=self._job_queue,
            message_puller="FakeUserDefinedMessagePuller",
        )
        self._threads.append(self._message_puller_thread)

        self._distributor_thread = JobDistributor(
            queue_in=self._distributor_status_queue,
            job_queue_in=self._job_queue,
            n_initial_workers=ElasticApp.NUMBER_OF_WORKERS,
            max_workers=ElasticApp.MAX_NUMBER_OF_WORKERS,
            worker_queue_size=ElasticApp.WORKER_QUEUE_SIZE,
            job_distribution_batch_size=ElasticApp.JOB_BATCH_SIZE,
        )
        self._threads.append(self._distributor_thread)

    def start(self) -> None:
        for thread in self._threads:
            thread.start()
        self.logger.info(f"{self._identity} - started Puller and Distributor")

    def stop(self) -> None:
        self._puller_status_queue.put("STOP")
        self._distributor_status_queue.put("STOP")
        self.logger.info(
            f"{self._identity} - STOP messages sent to the Puller and "
            f"Distributor. Joining..."
        )
        for thread in self._threads:
            thread.join()
        self.logger.info(f"{self._identity} - Threads joined!")

    def increase_load(self) -> None:
        self._puller_status_queue.put("INCREASE_LOAD")

    def decrease_load(self) -> None:
        self._puller_status_queue.put("DECREASE_LOAD")
