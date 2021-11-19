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