Example of how not to process events. Naive attempt to see what python is capable of before 
trying out proper technologies for events processing.

Direction:
- Consume a message from a pubsub topic
- Process the message (score a model? Get info from somewhere else before scoring?)
- Output the result to
    - Another pubsub topic
    - BigTable
  

Workers:

Each worker runs in a separate thread and can accept any message consumer, 
message processor or result processor provided that their implementations 
implement appropriate interfaces.

Processes:

The same application could be spawned across multiple processes. Each application
runs an instance of pubsub client, model for scoring and big table client.

*TODO*: 
- Big table + Pubsub publisher (different destination)
- Load testing
- Debugging
- Multiprocessing test

*TO THINK*:
- If one worker fails, how do I kill the other ones? Some channel to pass the
message back to the App, so it kills the remaining workers?
  
- How to implement scalability? Spawn new message processing processes when
there are many messages in the queue and kill them when not needed? Queues? If
  queues to the currently running processes are getting full, spin up a 
  new process. Round robin?
  
Multiple message consumers putting message in one large queue before distributing
messages across N workers?

Read messages -> message queue -> Distributor monitors currently running processes and
distributes messages between them using queue -> if queues are full, add a new processor / if 
queues are empty kill a running processor once its done with its messages