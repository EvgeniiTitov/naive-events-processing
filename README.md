- Start simple, with something you know
    - Use python's multiprocessing to create message consumers.
    

- Consume a message from a pubsub topic
- Process the message
- Output the result to
    - Another pubsub topic
    - BigTable
  

Workers:

Each worker runs in a separate thread and can accept any message consumer, 
message processor or result processor provided that their implementations 
implement appropriate interfaces.

Processes:

TBA