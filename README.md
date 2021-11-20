This is an example of how not to process events. Naive attempt to see what python is capable of before 
trying out proper technologies for events processing.

 
This events processing Application has 3 major parts:

1) Message consumer - could be anything as long as it implements the appropriate interface. For now its PubSub

2) Message processor - could be any model as long as it implements the appropriate interface. For testing purposes the IRIS classifier is used, could be any ML model

3) Results publisher - could be anything as long as it implements the appopriate interface. Currently, implemented: BigQuery, PubSub, BigTable (pending permissions)

Each part is run by a dedicated worker (thread). Workers are connected by queues, which allows them to exchange information.

Python's GIL shouldnt be a problem as we are primarily dealing with IO bound tasks (probably except for the model scoring part but it becomes IO
as well if we were to use, say, GPU)

For scaling purposes more than one instance of the Application could be run using multiprocessing.

---
*Test results*:

Test 1
- Single core (single app instance) processed 235 messages in 60 seconds (batch size 1)
- 4 cores (4 app instances) processed 998 messages in 60 seconds (batch size 1)

==> Scaling using more app instances is possible

Test 2
- Single core (single app instance) processed 235 messages in 60 seconds (batch size 1)
- Single core (single app instance) processed 691 messages in 60 seconds (batch size 3)

==> Scaling by reading (receiving) a batch of messages is possible

Test 3

TBA

---

*TODO*:

- Identify drawbacks and limitations of such approach
- Add big table to result_publisher
- Consider adding some feature enrichment step from BigTable
- Measure queue sizes, it seems read messages is quick, scoring is obviously slower, so its a bottleneck

---
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