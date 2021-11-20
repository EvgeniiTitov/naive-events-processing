class Config:
    DEBUG = False

    Q_SIZE_CONSUMER_TO_PROCESSOR = 50
    Q_SIZE_PROCESSOR_TO_PUBLISHER = 50
    PROJECT_ID = "gcp-wow-rwds-ai-mlchapter-dev"

    CONSUME_TOPIC_ID = "etitov-poc-sample-topic"
    CONSUME_SUBSCRIPTION_ID = "etitov-poc-sample-topic-sub"
    CONSUME_NUM_MESSAGES = 3

    # For message_processor.pubsub
    PUBLISH_TOPIC_ID = "etitov-poc-sample-topic-output"
    PUBLISH_NUM_MESSAGES = CONSUME_NUM_MESSAGES

    # For message_processor.big_query
    BQ_TABLE = (
        "gcp-wow-rwds-ai-mlchapter-dev.training.etitov-poc-events-output"
    )
