'''
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
'''