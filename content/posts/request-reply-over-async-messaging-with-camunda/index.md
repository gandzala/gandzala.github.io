---
date: '2026-10-16T15:41:00+02:00'
draft: false
title: 'Request-Reply over Asynchronous Messaging with Camunda 8'
---

Request-reply is one of the oldest integration patterns.
Running it over asynchronous messaging, with a request topic, a reply topic,
and a correlation key tying the two together, keeps the two systems decoupled.
They don't have to be available at the same time,
a burst of requests (or a thundering herd of retries after an outage)
waits in the topic instead of hitting the external system all at once,
and every request and reply becomes an event the rest of an event-driven architecture can observe.

Something has to remember that a request is waiting for its reply
and decide what happens when the reply is late or never comes.
That is exactly what a process engine is for.
I believe Camunda 8 is not just a tool for slow, human-centric business processes.
Its partitioned, horizontally scalable engine, Zeebe, is fast enough to orchestrate straight-through processing as well.

The catch is that combining asynchronous messaging with the distributed architecture of Camunda 8
creates a race condition that is easy to miss and hard to reproduce.
Let me demonstrate it on a simple example against Camunda 8.10.

## Simple implementation

Let's say a
[process solution](https://docs.camunda.io/docs/components/best-practices/development/writing-good-workers/)
needs to send a request through a Kafka topic and
wait for the reply in another topic. The simplest way to model such communication in BPMN
looks something like this:

{{< figure src="simple_message_intermediate_event.png" alt="A simple approach using message intermediate throw and catch events." width="317" align="center" >}}

Here is an example implementation of "Send request" and "Reply received" in Java using
[camunda-spring-boot-starter](https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/getting-started/)
and
[spring-kafka](https://docs.spring.io/spring-boot/reference/messaging/kafka.html).

```java
// Job worker implements "Send request"
@JobWorker                                                      // (1)
public void sendRequest(@Variable Request request) {
    kafkaTemplate.send(REQUEST_TOPIC, request)                  // (2)
            .join();                                            // (3)
}                                                               // (4)
```

```java
// Kafka listener publishes messages targeting "Reply received"
@KafkaListener(topics = REPLY_TOPIC)
public void listen(@Payload Reply reply) {                      // (8)
    camundaClient.newPublishMessageCommand()
            .messageName("Msg_ExternalSystemReplyReceived")
            .correlationKey(reply.correlationKey())
            .send()                                             // (9)
            .join();
}
```

### Under the hood
Let's peek at what is (or might be) happening under the hood.
For clarity, I will omit some details you should already be familiar with, like
[Zeebe brokers and gateways](https://docs.camunda.io/docs/components/zeebe/technical-concepts/architecture/).

{{< figure src="sequence_diagram.png" alt="Sequence diagram" align="center" >}}

When the process instance's
[token](https://docs.camunda.io/docs/reference/glossary/#token-process-instance)
arrives at "Send request",
Zeebe creates a new job, and one of the process solution's job workers activates it (1).
All of that is handled for us by `camunda-spring-boot-starter` and its `@JobWorker` annotation.

The job worker then publishes a message to the Kafka topic (2) and waits for the acknowledgement (3)
to make sure the message was written to the topic.
Only then may the job worker tell Zeebe that the job is complete (4).

When Zeebe receives the information that the job is complete,
it moves the token to the next element, "Reply received",
which makes the engine create a message subscription (5).

In the meantime, the external system picks up the request from the Kafka topic (6),
processes it, and publishes a reply (7).

After the process solution's Kafka listener reads the reply (8), it publishes a message to Zeebe (9),
and Zeebe correlates the message with the subscription.
The subscription is then closed and the token moves from "Reply received" to the next element.

### Potential problems
Once the request appears in the Kafka topic, its processing (6-9) runs in parallel with the rest of the steps (2-5).
Therefore, the process solution may publish the message to Zeebe (9) before the subscription is created (5).
Note that until the "Send request" job is completed successfully (or the token is moved manually),
the "Reply received" element is not activated and the subscription cannot be created.

Let's explore why steps (2-5) could get delayed.

#### Problem #1: Kafka producer was slow to receive an acknowledgement
After the Kafka producer sends the message to Kafka (2),
it can take a while to receive an acknowledgement (3).
If the acknowledgement does not arrive within
[`request.timeout.ms`](https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_request.timeout.ms)
(30 seconds by default), the producer sends the message (2) again, and keeps retrying
(see [`retries`](https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_retries) and
[`delivery.timeout.ms`](https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_delivery.timeout.ms)),
even though one of the previous attempts may have already succeeded.
With [`enable.idempotence`](https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_enable.idempotence)
the broker discards such duplicates, but the job worker still has to wait.

After
[`delivery.timeout.ms`](https://kafka.apache.org/43/configuration/producer-configs/#producerconfigs_delivery.timeout.ms)
(2 minutes by default), the send fails and Problem #1 turns into Problem #2.

#### Problem #2: Job failed
After publishing the request (2), the job worker failed the job (see
[Completing or failing jobs](https://docs.camunda.io/docs/components/concepts/job-workers/#completing-or-failing-jobs)).
The job could have been failed explicitly by the programmer
or implicitly by the Camunda SDK when it caught an unhandled exception caused, for example, by
a programming error (e.g., `NullPointerException`),
a lack of resources (e.g., disk space), or
the Kafka producer (e.g., `TimeoutException`, `NetworkException`, `NotEnoughReplicasAfterAppendException`).
If the job has no retries left, a
[Camunda incident](https://docs.camunda.io/docs/components/concepts/incidents/)
is created, and there is usually no upper limit on how long it will take someone to resolve it.
Even if the job still has retries left,
Zeebe will wait for a retry backoff (if set) before the job can be activated again (1).
And there is no guarantee that the root cause (Kafka, network, resources, etc.) has been resolved by then
and the job will not fail again.

#### Problem #3: Job worker crashed or was unable to complete the job
After publishing the request (2), the job worker was forcefully terminated,
for example, by a `SIGKILL` from the kubelet or OOM killer,
or because a JVM started with `-XX:+ExitOnOutOfMemoryError` ran out of memory.
By default, it takes 5 minutes (see
[camunda.client.worker.defaults.timeout](https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/properties-reference/#camundaclientworkerdefaultstimeout))
before Zeebe lets another job worker activate the job and possibly complete it.

#### Problem #4: Job worker was slow to complete or fail the job
The job worker could have been slowed down by CPU exhaustion, garbage collection,
memory paging (swapping), or network issues.
All communication goes through a Zeebe gateway, which can experience performance issues as well.
A Zeebe partition processes commands in the order they arrive,
so the complete job command has to wait until all commands that arrived before it are processed.
Even if
[backpressure](https://docs.camunda.io/docs/components/zeebe/technical-concepts/internal-processing/#handling-backpressure)
is on, the command has to wait for complete job commands that arrived before.

#### Problem #5: Zeebe was slow to create a message subscription
With some exceptions, you have to assume that the process instance and the message subscription
may belong to different Zeebe
[partitions](https://docs.camunda.io/docs/components/zeebe/technical-concepts/partitions/).
When that happens, the partition the process instance belongs to
has to tell a different partition (determined by the correlation key) to create the message subscription.
Whether there is an error or a delay (for example, during
[rebalancing](https://docs.camunda.io/docs/self-managed/components/orchestration-cluster/zeebe/operations/rebalancing/)),
the first partition does not wait.
It just keeps retrying in the background until it receives an acknowledgement from the other partition.

#### Problem #6: A design or programming error
Right now, everything may be just fine. But in the future,
a new team member (or an AI) might decide to add some extra logic to the job worker
right after the request is published (2) or acknowledged (3).
Or they might add a job worker between "Send request" and "Reply received",
either directly to the diagram or as a sneaky
[execution listener](https://docs.camunda.io/docs/components/concepts/execution-listeners/).
That would add extra steps that might need retries, create Camunda incidents,
or delay the processing in other ways.

#### Problem #7: Zeebe was restored from a backup
For completeness, it is worth mentioning that restoring a Zeebe cluster
from a backup might have a similar effect.
If Zeebe state rolls back while Kafka offsets don't,
replies arrive for subscriptions that have not been opened yet.

### How the problem manifests
Publishing a message to Zeebe is fire-and-forget.
Zeebe accepts the message regardless of whether a matching subscription exists.
Therefore, when the reply message arrives *long* before the subscription is created,
there is no error, no Camunda incident, and not even a metric that would indicate something is out of place.
I will cover what *long* means in more detail later in [Approach #1](#approach-1-buffer-messages-in-zeebe).

The process instance might get stuck, or it might even take a different path in the BPMN diagram
because the reply was not correlated in time. For example:

{{< figure src="example_issue_timer.png" alt="A process waiting for a reply with a timer boundary event that cancels the order." width="360" align="center" >}}

In the example above, if the reply message does not get correlated and we do not fix that before the timer fires,
the "Cancel order" task is executed.
Immediately after that, the process instance (or the given scope) completes.

Some of the problems above can be hard to catch (or even reproduce) in dev or test environments.

### Fixing affected process instances
If we find a process instance that is waiting for a message that arrived *long* ago,
we can publish the message and its data to Zeebe manually (using the
[Publish message API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/publish-message/)),
or edit the process variables and move the token (using the
[Modify process instance API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/modify-process-instance/)
or
[Camunda Operate](https://docs.camunda.io/docs/components/operate/operate-introduction/)).

If there are too many such process instances, or we cannot determine which ones are affected,
we can sometimes replay all messages in the reply topic from a certain point.
I will talk about the associated caveats [later](#buffering-caveat-2---message-duplication).

## Mitigations and alternative approaches
This is one of those issues with a low probability of happening
that usually requires a lot of senior time to uncover and safely fix,
provided there is still enough data to do so.
So let's see what we can do (or are already doing) to lower the probability or fix the issue completely.

### Approach #1: Buffer messages in Zeebe
You might be asking: "But wait, I am already using this pattern and everything seems to work. How is that?"
Sometimes, the external system is just that slow, and the reply takes anywhere from minutes to days.
But most likely, it works because Zeebe buffers the messages your Kafka listener publishes.

Zeebe can buffer published messages for a period set by their time-to-live (TTL) (see
[message buffering](https://docs.camunda.io/docs/components/concepts/messages/#message-buffering)).
If the subscription opens within this time, the message is correlated.

Expanding on the example Kafka listener above,
this is how you can set the TTL explicitly:
```java
@KafkaListener(topics = REPLY_TOPIC)
public void listen(@Payload Reply reply) {
    camundaClient.newPublishMessageCommand()
            .messageName("Msg_ExternalSystemReplyReceived")
            .correlationKey(reply.correlationKey())
            .timeToLive(Duration.ofHours(2)) // <-- Setting TTL to 2 hours
            .send()
            .join();
}
```

If you do not set the TTL explicitly while building the publish message command,
the configuration value `camunda.client.message-time-to-live` is used.
If that is not set either, the default of 1 hour applies (see the
[docs](https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/properties-reference/#camundaclientmessagetimetolive)).

A TTL of 1 hour should cover most of the issues mentioned above, but no finite value will cover all of them.
There is no upper limit on how long the processing can get stuck due to a Camunda incident,
Kafka downtime, or issues with the process solution, Zeebe, or the underlying infrastructure.

#### Buffering Caveat #1 - Memory and disk
Messages stay in the buffer (i.e., in RocksDB state) for the full TTL, even after they have been correlated.
Therefore, processing messages affects not only CPU but also memory and disk.
While the impact on CPU is roughly proportional to the number of messages,
the impact on memory and disk is also proportional to their TTL and payload size.

Also, be very careful not to publish irrelevant messages.
For example, if the topic contains replies to requests from other systems,
the Kafka listener must be able to filter them out.

When designing a performance test, keep in mind that the buffer reaches its maximum size after TTL.
From then on, messages expire at roughly the rate they arrive.
The test therefore has to run noticeably longer than the TTL to show the steady state.

#### Buffering Caveat #2 - Message duplication
There are many reasons why a single message can get published multiple times. For example:
* The external system does not support idempotence and produced a reply more than once.
* The Kafka listener publishes the message multiple times.
  For example, due to network issues or because the listener fails before it commits the offset.
* The Kafka listener's consumer group offset is reset (i.e., the replies are replayed).
  Whether during a disaster recovery, after a bug fix, or just because some of the replies were not correlated previously.

Duplicate messages do not only affect memory and disk. They can produce unwanted side effects as well.
Let me demonstrate:

{{< figure src="two_subscriptions_in_loop.png" alt="Two subscriptions in a loop." width="566" align="center" >}}

Zeebe never correlates a message to the same process twice
(see [message cardinality](https://docs.camunda.io/docs/components/concepts/messages/#message-cardinality)),
but a duplicate is a different message.
If "Reply received" reuses the same correlation key in each iteration and one of the replies is duplicated,
the next "Reply received" subscription is immediately correlated with the duplicate carrying the old data.
Here it is rather obvious that the subscriptions might consume the duplicate.
In reality, with call activities, collapsed sub-processes, link events, etc., it may be harder to spot.

At a minimum, the Kafka listener should publish the messages with a
[message ID](https://docs.camunda.io/docs/components/concepts/messages/#message-uniqueness).
Ideally, the Kafka listener should calculate the message ID deterministically from the reply.
Keep in mind that Zeebe only rejects a duplicate message ID while the original message is still buffered.

If you want a more bulletproof solution,
I suggest generating a unique correlation key before running "Send request".
The key can then be used by Kafka producer, the external system, and the Kafka listener
to ensure end-to-end idempotency.

#### Buffering Caveat #3 - Testing
Imagine you start a test suite and something breaks midway.
So you fix it, delete all process instances, clean the Kafka topics and the external system, and run the test suite again.
But the previous run could have left an uncorrelated message in Zeebe.
For example, because the test was terminated before the message subscription was created or because the message was duplicated.
If the next run starts within the message's TTL, it might be affected.
Something that is broken may look like it works, or something that works may look broken.

Generating a new unique correlation key before "Send request" fixes this.
Alternatively, you may consider lowering the TTL for non-performance tests.

#### Buffering Caveat #4 - Configuring TTL
It may look like a small detail at first, but how do you even choose the correct value for the TTL?
Would you use different TTLs for different messages? On different environments?
If you rely heavily on the TTL, you have to document and enforce the setting.

### Approach #2: Create the message subscription in parallel with sending the request
The idea is to get the token to "Reply received" without waiting for "Send request" to complete, or even to start.

{{< figure src="mitigation_parallel_gateway.png" alt="A problem mitigation using a parallel gateway." width="346" align="center" >}}

First, let me address two small critiques I often hear.

The correlation key for "Reply received" cannot be generated by "Send request"
and it must be produced before either of them runs.
But there are other reasons to generate a unique correlation key in a step before "Reply received" anyway,
see [Buffering Caveat #2](#buffering-caveat-2---message-duplication).

The diagram becomes harder to read.
But only because the original diagram was, in a sense, lying.
Given the asynchronous nature of the communication,
we cannot wait until the request has been sent before we start listening for the reply.
`ReplyingKafkaTemplate`, for example, doesn't do that either.
So even if message buffering makes "Send request" and "Reply received" work sequentially,
logically they must behave as if they run in parallel.

But the main question is: Does it solve the problem? Almost.

Issues related to Kafka and the Kafka producer (Problem #1) are no longer a concern.
Neither is the "Send request" job running into any kind of problem (Problems #2, #3, and #4).

This approach will not help us deal with Zeebe being restored from a backup (Problem #7).
And when it comes to programming errors (Problem #6), you can argue both ways.
The added complexity increases the chance of an error.
But now you can safely add code after sending the request: directly to the job worker, as an execution listener, or as an additional step after "Send request".

That leaves us with delays caused by Zeebe itself (Problem #5).
For most applications, message buffering is a valid tool for that, provided you build and test your cluster for adequate availability.
Just make sure the TTL corresponds to your operational level agreements.

### Approach #3: Poll the external system
No process solution lives in a vacuum, and there could be other reasons
for the reply message not to arrive that are outside our control.
In that case, we might tackle more than one problem by periodically polling the external system.
BPMN is actually very handy for modeling such logic:

{{< figure src="mitigation_polling.png" alt="Polling the external system for the response." width="566" align="center" >}}

The non-interrupting timer boundary event repeatedly triggers "Query the external system",
which checks whether the request was processed (or another condition was met).
If so, the job worker can publish an appropriate message to Zeebe for the "Reply received" subscription or use a
[terminate event](https://docs.camunda.io/docs/components/modeler/bpmn/terminate-events/)
to end the BPMN scope.

The timer is the hardest part to design.
The polling interval must be short enough to keep the delay between a reply that failed to correlate
and the query that finds it acceptable.
But if the interval is too short, we might overwhelm the external system with unnecessary queries.

We also lose some of the protection Kafka gives us against the thundering herd effect.
Whenever the external system falls behind on a burst of requests,
or Camunda comes back after an outage and fires all the overdue timers at once,
we have to make sure we do not send too many queries at the same time.

Strategies to consider are adding jitter to the interval, increasing the interval exponentially,
and limiting how many "Query the external system" jobs run concurrently.

### Approach #4: Retry sending the request
Sometimes, not only the reply but also the request itself could get lost.
For example, when the Kafka producer uses `acks=0` or `acks=1`,
when the Kafka topic uses `min.insync.replicas=1`,
after Kafka is restored from a backup, etc.
In such cases, we should consider retrying sending the request.

{{< figure src="mitigation_retry.png" alt="Retrying the request." width="576" align="center" >}}

In this case, we have to make sure that
1) the new request gets through (and is not discarded as a duplicate, for example, by an idempotency check),
2) the external system can handle multiple requests properly, and
3) the external system sends a reply for each request it receives.

## Honorable mentions
### Implement the request-reply pattern outside Camunda
Steps "Send request" and "Reply received" can be implemented by a single job worker using, for example,
[ReplyingKafkaTemplate](https://docs.spring.io/spring-kafka/reference/kafka/sending-messages.html#replying-template).
That approach comes with its own challenges:
* If the process solution runs in multiple instances, you have to carefully manage reply topics and consumer groups.
* Zeebe makes the job available to another job worker once its `timeout` expires.
  `ReplyingKafkaTemplate::sendAndReceive` should therefore either be used with an appropriate `replyTimeout`, or
  the job worker should periodically extend the job's `timeout` using the
  [Update job API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/update-job/).
* If the job worker crashes, Camunda will retry the job. The new request must get to the external system
  (i.e., must not be discarded as a duplicate), and it must produce a new reply,
  even if the old request was already processed and its reply sent.

For this to work, we have to make sure that we get a timely reply to every request, even a duplicated one.
In that case, I would suggest considering REST or gRPC instead of Kafka.
Camunda features like job retries and backoff periods can alleviate some of the pain of tighter coupling.

### Create the message subscription before sending the request
The idea is to make sure the message subscription ("Reply received") is open before the request is sent ("Send request").
For example, that is what `ReplyingKafkaTemplate` does.

The problem is modeling such behavior in BPMN.
We could, for example, try something like this:

{{< figure src="mitigation_boundary_event.png" alt="A problem mitigation using a boundary event." width="497" align="center" >}}

Note that the "Reply received" element is a boundary event,
so the Zeebe engine processes it before it activates anything inside the expanded sub-process.
After "Send request", the "No-op" element just waits for an event that never arrives, in order to keep
the sub-process and its boundary message event alive.

Not only does it break the KISS principle, it doesn't give much stronger guarantees than
[Approach #2](#approach-2-create-the-message-subscription-in-parallel-with-sending-the-request).

### Correlate the message instead of publishing it
The idea is that if the Kafka listener (in the process solution) used the
[correlate message API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/correlate-message/)
instead of the
[publish message API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/publish-message/),
it would know when the message was not correlated with any message subscription.

But it would not know why.
If the subscription has not been created yet,
the message needs to be moved to a different topic or storage for later processing
(otherwise, the message either gets lost or becomes a poison pill).
If the message is a duplicate
(for example, the Kafka listener processed the message but failed just before committing the offset),
it should be discarded.

### Do not wait for the acknowledgement of the request
Let's say you change the implementation of "Send request" like this:
```java
@JobWorker
public void sendRequest(@Variable Request request) {
  kafkaTemplate.send(REQUEST_TOPIC, request); // <-- There is no join()
}
```

It does not fully solve any of the Problems #2-#7,
and it removes the guarantee
that after "Send request" completes, the request was actually written to the Kafka topic.

### Stay in the same partition
This is an attempt to improve [Approach #2](#approach-2-create-the-message-subscription-in-parallel-with-sending-the-request)
by making sure both the process instance and the subscription are processed by the same partition.

Each Zeebe partition is a record stream processed by a single stream processor
(see [Internal processing](https://docs.camunda.io/docs/components/zeebe/technical-concepts/internal-processing/)).
So if all the processing happens in a single partition, we remove some of the race conditions,
because there is only logical concurrency.

On top of that, Zeebe uses
[batch processing](https://github.com/camunda/camunda/issues/11416):
the stream processor keeps processing the follow-up commands of a command until it reaches a wait state
or the limit set by
[`camunda.processing.max-commands-in-batch`](https://docs.camunda.io/docs/self-managed/components/orchestration-cluster/zeebe/configuration/broker-config/#camundaprocessing).
As long as the parallel gateway and both of its branches are processed in the same batch,
creating the "Send request" job and opening the subscription should happen atomically.
Hence, the publish message command would be processed after the subscription is already opened.

Note that this behavior is not documented,
and I might have made a mistake when analyzing and testing the code.

Nevertheless, if this is true, there are two interesting implications:
* We can use the pattern from Approach #2 while publishing messages with TTL = 0.
  That would free us from most of the caveats related to message buffering.
* It is cheaper and faster:
  no extra space for the message in the buffer, no message expiration, and no inter-partition communication.

The last question is how to ensure the subscription is opened in the same partition
as the process instance.

If we create the process instance using a message with a correlation key
and then use the same correlation key for "Reply received",
the message subscription is guaranteed to be on the same partition:

> When a client publishes a message to trigger a message start event,
> the message is forwarded to a partition based on the correlation key of the message.
> The process instance is created on the same partition where the message is published.

Source: [Partitions#Distribution over partitions](https://docs.camunda.io/docs/components/zeebe/technical-concepts/partitions/#distribution-over-partitions)

I suspect similar logic applies to starting a process instance with the
[Create process instance API](https://docs.camunda.io/docs/apis-tools/orchestration-cluster-api-rest/specifications/create-process-instance/)
and its `businessId` parameter (available since Camunda 8.9):

> When a business ID is specified, the partition for the new process instance
> is determined deterministically by hashing the business ID,
> rather than using the default round-robin distribution.
> This ensures that uniqueness checks occur on a single partition.

Source: [Process instance creation#Uniqueness control](https://docs.camunda.io/docs/components/concepts/process-instance-creation/#uniqueness-control)

Two caveats remain:
1) There is no documented way to generate a new UUID that maps to a given Zeebe partition.
   But it seems like we can overcome this by using `businessId` in the published message (since Camunda 8.10).
2) Process instances started by a message with a correlation key or with a business ID always start
   in the partitions the Zeebe cluster was initially created with. That reduces the benefits of
   [dynamic partition scaling](https://docs.camunda.io/docs/reference/announcements-release-notes/880/880-release-notes/#dynamic-partition-scaling)
   (available since Camunda 8.8).

Let me know if you would be interested in exploring this idea in greater detail.

## Summary

There is no golden hammer here.
Every process solution lives with different expectations, in a different environment, with a different set of resources,
so instead of a single recommendation, here is what I would like you to take away:

* Sending a request and waiting for its reply over asynchronous messaging are, logically, concurrent operations.
* Message buffering is what makes the naïve model work most of the time.
  But it is a probabilistic fix: no finite TTL covers everything.
* Camunda chose performance and scalability.
  Each Zeebe partition is a Raft group with its own strongly consistent log,
  but partitions talk to each other asynchronously, with retries and no upper bound on delay.
  Opening a subscription requested by another partition is only eventually consistent.
* Generate a unique correlation key before sending the request.
  It is the cheapest change with the most benefits:
  end-to-end idempotency, no collisions between loop iterations or test runs.
* Consider alternative approaches instead of the naïve one.
