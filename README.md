# Go RMQ

Simple rabbitmq pub/sub client based on [https://github.com/wagslane/go-rabbitmq](https://github.com/wagslane/go-rabbitmq).

# Flow

``` mermaid
flowchart TB
    Assumptions@{ shape: brace-r, label: "
    -- Connection and channel management is not included in this diagram
    -- On disconnect/channel close event, a re-connect process must be executed
    -- On re-connect consumers must be re-run
    -- Prefetch count should be configurable through env or app config
    -- DLQ creation, publishing to DLQ must be done using the same flow" }

    RMQServer["RabbitMQ Server"] <--Pub/Sub--> App
    App ---> StartRetryRoutine["Start thread/routine
    to retry failed to
    publish messages"]

    

    subgraph RetryBlock [Retry Failed Messages]

        StartRetryRoutine ---> IntervalCheck{"Has interval
        elasped?"}

        IntervalCheck --Yes--> LockQ1ToRetry["Lock Q1"] 
        LockQ1ToRetry ---> Retry["Retry messages 
        from failed to 
        publish queue"]
        Retry ---> PushReFailed["Push failed to 
        publish messages 
        in new queue (Q2)"]
        PushReFailed ---> ReplaceFailedQueue["Replace Q1 with Q2"]
        ReplaceFailedQueue ---> UnlockQ1AfterRetry["Unlock Q1"]
        UnlockQ1AfterRetry ---> DoneRetry([Done])


        IntervalCheck --"No"--> NoOp["No action"]
        NoOp ---> DoneNoOp([Done])
    end

    App ---> Publish

    subgraph Pub [Publish]

        Publish ---> TryPublish["Try to publish 
        immediately"]
        TryPublish ---> PubFailedCheck{"Failed to 
        publish?"}
        PubFailedCheck --Yes--> LockQ1ToPush["Lock Q1"]
        LockQ1ToPush ---> AddToQueue["Add failed message
        to queue (Q1)"]
        AddToQueue ---> UnlockQ1AfterPush["Unlock Q1"]
        UnlockQ1AfterPush ---> DoneTryPub([Done])
        PubFailedCheck --"No"--> DonePub([Done])
    end

    App ---> AddConsumer[Add consumer]

    subgraph Consume [Consume]
        AddConsumer ---> DlqCheck{"Create DLQ?"}

        DlqCheck --Yes--> CreateDlq["Create DLQ 
        with name in this format
        ***service-name:route:dlq***"]
        CreateDlq ---> BindDlq["Bind DLQ to a 
        DLQ route with name in 
        this format
        ***service-name:dlq-route***"]
        DlqCheck --"No"--> SetupPrefetchCnt["Setup a non-zero
        prefetch count per consumer"]
        BindDlq ---> SetupPrefetchCnt

        SetupPrefetchCnt ---> SetupConsumerQueue["Create persistent
        consumer queue with
        name in this format
        ***service-name:route:queue***"]
        SetupConsumerQueue ---> StartConsumer["Start consumer"]
        StartConsumer ---> NewMsg["New message"]

        NewMsg ---> ProcessMsg["Process message"]
        ProcessMsg ---> ProcessFailedCheck{"Processing failed?"}
        ProcessFailedCheck --Yes--> HasDlqCheck{"Has associated
        DLQ?"}
        ProcessFailedCheck --"No"--> Ack["Acknowledge
        delivery"]
        Ack ---> DoneConsuming([Done])
        HasDlqCheck --Yes--> PublishToDlq["Publish message to 
        associated DLQ route"]
        PublishToDlq ---> Ack
        HasDlqCheck --"No"--> Ack
        DoneConsuming ---> NewMsg
    end
```

## Rationale

This is made for a specific project need. As we are using rabbitmq server of 
version lower than 4 and also we don't have much option to customize it,
we had to implement something that could provide the pub/sub interface
our application needs. So it is not intended for any generic usecase.
