"""
POC - Naive elastic app

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
# TODO: Cant pass user defined objects to the worker

import time
import multiprocessing
import threading
import typing as t
import queue
import random

from app.helpers import LoggerMixin, get_pid_number
from .abstractions import AbsMessageConsumer, message
from app.message_consumer import PubSubMessageConsumer
from app.message_processor import IrisClassifier
from app.result_publisher import BigQueryMessagePublisher


Batch = t.List[t.Optional[message]]


class Worker(multiprocessing.Process, LoggerMixin):
    """
    The Worker object running in a separate process that receives jobs
    (messages) from the JobDistributor via a queue. The Worker uses the user
    provided objects MessageProcessor and ResultPublisher to process the task
    """

    def __init__(
        self,
        job_queue: multiprocessing.Queue,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._job_queue = job_queue
        self._message_processor = None
        self._result_publisher = None
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

        self._message_processor = IrisClassifier()
        self._result_publisher = BigQueryMessagePublisher()

        while True:
            task: t.Any = self._job_queue.get()
            if "STOP" in task:
                break
            self._process_message(task)

            # TODO: Delete me
            time.sleep(2)

            self.logger.info(
                f"{self._iam} - processed task {task}; "
                f"My queue size: {self.job_queue.qsize()}/{queue_capacity}"
            )
        self.logger.info(f"{self._iam} - stopped")

    def _process_message(self, message: message) -> None:
        try:
            result = self._message_processor.process_messages([message])
        except Exception as e:
            self.logger.exception(
                f"{self._iam} - {self._message_processor.__class__.__name__} "
                f"failed while processing message {message}. Error: {e}"
            )
            raise e
        if not result:
            return
        try:
            self._result_publisher.publish_result(result)
        except Exception as e:
            self.logger.exception(
                f"{self._iam} - {self._result_publisher.__class__.__name__} "
                f"failed while publishing results {result}. Error: {e}"
            )
            raise e


class MessagePuller(threading.Thread, LoggerMixin):
    """
    MessagePuller object running in a thread in the main process whose main
    task is it receive messages from the MessageConsumer provided and put them
    in the job queue connected to the JobDistributor
    """

    def __init__(
        self,
        queue_in: queue.Queue,
        job_queue_out: queue.Queue,
        message_puller: AbsMessageConsumer,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._job_queue_out = job_queue_out
        self._message_puller = message_puller

        self._batch_being_distributed: Batch = []
        self._message_being_sent = None

        self._pid = get_pid_number()
        self._iam = f"PID: {self._pid} - MessagePuller"
        self.logger.info(f"{self._iam} inited")

    def run(self) -> None:
        while True:
            stop = self._check_if_time_to_stop()
            if stop:
                break  # It is assumed we just ignore pulled messages if any
            if not len(self._batch_being_distributed):
                batch = self._get_new_batch_of_messages()
                if len(batch):
                    self._batch_being_distributed = batch
                else:
                    continue
            if not self._message_being_sent:
                self._message_being_sent = self._batch_being_distributed.pop()
            try:
                self._job_queue_out.put(self._message_being_sent, timeout=0.3)
            except Exception:
                pass
            else:
                self._message_being_sent = None
        self.logger.info(f"{self._iam} stopped")

    def _check_if_time_to_stop(self) -> bool:
        try:
            task = self._queue_in.get_nowait()
        except queue.Empty:
            return False
        else:
            if "STOP" in task:
                return True
            else:
                self.logger.info(f"{self._iam} - got unknown task")
                return False

    def _get_new_batch_of_messages(self) -> Batch:
        try:
            messages: Batch = self._message_puller.get_messages()
        except Exception as e:
            self.logger.exception(
                f"{self._iam} - failed while pulling messages. Error: {e}"
            )
            raise e
        return messages


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
        """
        Returns a worker following the Round Robin approach.
        Shuffles the workers at the beginning to ensure the first ones do not
        get busier than the ones closer to the end of the list
        """
        random.shuffle(self._running_workers)
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
        worker_to_stop = self._running_workers.pop(
            self._get_index_of_least_busy_worker()
        )
        worker_to_stop.job_queue.put("STOP")
        self._stopping_workers.append(worker_to_stop)
        self.logger.info(
            f"{self._iam} - scaling down started, signalled worker to stop"
        )

    def _get_index_of_least_busy_worker(self) -> int:
        index_load = [
            (i, self._running_workers[i].my_capacity[0])
            for i in range(len(self._running_workers))
        ]
        return min(index_load, key=lambda pair: pair[-1])[0]

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
        worker = Worker(job_queue=worker_job_queue)
        return worker


class ElasticApp(LoggerMixin):

    NUMBER_OF_WORKERS = 1
    MAX_NUMBER_OF_WORKERS = 8
    JOB_QUEUE_SIZE = 100
    WORKER_QUEUE_SIZE = 5
    JOB_BATCH_SIZE = 5

    def __init__(self):
        self._pid = get_pid_number()
        self._identity = f"PID: {self._pid} - ElasticApp"

        self._puller_status_queue = queue.Queue()
        self._distributor_status_queue = queue.Queue()
        self._job_queue = queue.Queue(ElasticApp.JOB_QUEUE_SIZE)
        self.logger.info(f"{self._identity} - Queues inited")

        self._threads = []

        self._pubsub = PubSubMessageConsumer()
        self._message_puller_thread = MessagePuller(
            queue_in=self._puller_status_queue,
            job_queue_out=self._job_queue,
            message_puller=self._pubsub,
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
