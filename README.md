# PopulateEventToNats
Test Harness to Publish Event History Message to NATS Streams for ST-EH. 
The  harness publishes a configurable rate of messages for a specified amount of time.

The Stream and the Durable Consumer are both expected to be created prior to running the test.

In order to distribute connections across the cluster all endpoints need to be passed via the cli. (using -s)

### Common Parameters
1. **-s** is the location of the NATs URL, if passing multiples, separate them by a space and enclose the entire list in quotes.
2. **-stream** The Name of the Stream to Publish to or Consume From
3. **-tst** is one of:
   1. **loadTest** - this is used to publish messages at a fixed rate for a set amount of time
   2. **consumeOnSchedule** this is used to launch a consumer every 30 seconds to read the number of messages in the -bs or until there are no more messages
   3. **neverClosingSingleConsumer** - this is used to launch a never ending consumer that simply continues to wait until more messages show up , but will read them in batches of -bs


### Publishing Related Parameters
1. **-ttm** Total Time in Minutes to run the *loadTest*
2. **-mps** Messages Per Second per Publisher during the *loadTest* 
3. **-nps** Number of Publishers to be used in the *loadTest*


### Consumer Related Parameters
1. **-consName** the name of the durable consumer that should be used when consuming messages
2. **-bs** batch size, the total number of messages to consume per connection. not to be confused with the batch size  that NATs uses internally
3. **-subject** The subject used when filtering the Events from the Consumer 

### Deploying the fat jar and running the test harness 
In order to execute the test harness easily in a deployed env where NATS is not exposed to an external IP.
1. Execute the fatJar gradle task, this will create a jar file under build/libs/PopulateEventToNats-1.0-SNAPSHOT.jar (or something very similar)
2. Copy the jar file to a temporary directory on one of the pods that has java already available (for example ocs-aio-0)
   1. if you have kubectl configured locally to access a remote env then you can use the following command from the build/libs directory
      1. `kubectl cp -n oce-dc01  PopulateEventToNats-1.0-SNAPSHOT.jar ocs-aio-0:/tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -c main`
3. Once copied to the remote pod, log into the pod
   1. if you have the oc cli tooling installed and configured you can use something like the following command
      1. `oc -n oce-dc01 -it exec ocs-aio-0 -c main -- bash`

### Examples
1. Run the *loadTest* at 800 messages per second for 10 minutes using 8 publishers, notice how all 3 nats pod endpoints are passed using -s, this distributes the connections evenly
   1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -s='ocernd06-eh-nats-0.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-1.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-2.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222' -tst=loadTest  -ttm=10 -mps=100 -np=8`
2. Consume the messages using a scheduled consumer - the connections will be opened and closed repeatedly
   1. Two things to note about the consumer - the subject really needs to match what the stream is configured with, otherwise there will be an error. If in doubt leave it out and use the default. Second is that the subject is surrounded with quotes because of the .> at the end
      1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -s='ocernd06-eh-nats-0.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-1.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-2.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222' -tst=consumeOnSchedule -stream=Events -consName=zdataDurName -bs=100000 -subject="Events.>"`
3. Consume the messages using a single never ending consumer - using a single durable connection
   1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -s='ocernd06-eh-nats-0.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-1.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222 ocernd06-eh-nats-2.ocernd06-eh-nats.ocernd06-eh.svc.cluster.local:4222' -tst=neverClosingSingleConsumer -stream=Events -consName=zdataDurName -bs=100000 -subject="Events.>"` 