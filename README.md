This is an example of how not to process events. Naive attempt to see what python is capable of before 
trying out proper technologies for events processing.

 
Events processing Application has 3 major parts:
1) Message consumer - could be anything as long as it implements the appropriate interface. For now its PubSub
2) Message processor - could be any model as long as it implements the appropriate interface. For testing purposes the IRIS classifier is used
3) Results publisher - could be anything as long as it implements the appopriate interface. Currently implemented: BigQuery, PubSub, BigTable (pending permissions)

Each part runs by a dedicated worker (thread). Workers are connected by queues, which allows them to exchange information.

Python's GIL shouldnt be a problem as we are primarily dealing with IO bound tasks (probably except for the model scoring but it becomes UI
as well if we were to use, say, GPU)

For scaling purposes more than one instance of the Application could be run using multiprocessing.



*TODO*:

- Add big table to result_publisher
- Load testing (messages per minute simple and multicore)
- Multiprocessing test
- Consider adding some feature enrichment step from BigTable

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