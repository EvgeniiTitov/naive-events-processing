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
# TODO: Do pipes have capacity like queues?! (size?)

import time
import multiprocessing
import threading
import typing as t
import queue

from app.helpers import LoggerMixin, get_pid_number


pipe_conn = multiprocessing.connection.Connection


class Worker(multiprocessing.Process, LoggerMixin):
    """
    Fake worker running in a separate process connected by the pipe to the
    Distributor sitting in the main process. The worker gets jobs to process
    through the pipe.
    """

    def __init__(
        self,
        job_pipe_end: pipe_conn,
        message_processor,
        result_publisher,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._pipe_end = job_pipe_end
        self._message_processor = message_processor
        self._result_publisher = result_publisher
        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - Worker inited")

    def run(self) -> None:
        while True:
            payload: t.List[t.Any] = self._pipe_end.recv()
            if len(payload) == 1 and "STOP" in payload:
                break

            # It is assumed only a single message can be sent through the pipe
            message = payload[0]
            print(f"PID: {self._pid} - processing message {message}")
            time.sleep(2)  # Long lasting message processing

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
        message_puller,
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
                payload = self._queue_in.get_nowait()
            except queue.Empty:
                pass
            else:
                if "STOP" in payload:
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
        job_queue: queue.Queue,
        n_initial_workers: int,
        *args,
        **kwargs,
    ) -> None:
        super(JobDistributor, self).__init__(*args, **kwargs)
        self._queue_in = queue_in
        self._job_queue = job_queue
        self._n_workers = n_initial_workers

        # TODO: 1. Start N workers
        #       2. You have to keep track of process objects + pipes

        self._pid = get_pid_number()
        self.logger.info(f"PID: {self._pid} - JobDistributor inited")

    def run(self) -> None:
        """
        Tasks:
        1. Check queue_in to stop when required
        2. Get a job from the job queue
            2.a Round robin the job to one of the running workers
        3. Check worker queues
            - Full? Create a new worker, add to the pull
            - Empty? Check if more than min N of workers, if yes send kill message

        """
        pass
