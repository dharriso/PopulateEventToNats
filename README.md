# PopulateEventToNats
Test Harness to Publish Event History Message to Partitioned NATS Streams for ST-EH. 
The  harness publishes a configurable amount of messages across all the partitions specified (normally 256) into Nats. 
There will be 1 Events stream for each partition specified. For example if the number of partitions is 256 then the test method createPartitionedStreams will create 256 streams, with the following naming convention of the stream

 - Event0, Event1, ...., Event255.

The subject for each event has the following critierai

 - 0.>, 1.>,...,255.>

The consumers durable name also follows a similar convention i.e.

 - durName0, durName1,....., durName255.

In addition to publishing event messages into all the Events streams 1 message will be publish into the stream named "TRANSFERS" with a subject of "TRANSFERS.>".
The message published here will have the subject set to  "Transfers.WRITING". This then will be used to orchestrate the export the data from NATS to disk

Typically we perform the following tasks

 1. Execute the harness once only with **tst=createPartitionedStreams** and partitions=256 which will create all the Event streams and the TRANSFERS stream e.g. **-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=createPartitionedStreams -timeId=66 -useId=22 -group=1**
 2. Execute the harness with **tst=publishPartitionedStreams** as many times as needed. Ensuring you specify a unique timeId and useId on the command line as shown above.
 3. Finally if you need to cleanup you can execute the harness **-tst=deletePartitionedStreams** 

The example below shows the command line arguments required

**-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=deletePartitionedStreams -timeId=66 -useId=22 -group=1**
**-s** is the location of the NATs URL
-**mc** **is the number of event messages to be populated into each partition**
**-timeId** is the timeId to use for the creation of each event. Events with the same timeId will be processed in any given interval.
**-useId** is the useId to use for the creation of each event.
**-group** is the groupdId to use for the creation of Transfer record. 
partitions is the number of partitions we have defined, usually 256
**-tst** is one of 
  **createPartitionedStreams** create a NATS stream for each of the partitions i.e. 256 partitioned streams
  **deletePartitionedStreams** delete the same create above.
  **publishPartitionedStreams** will publish "mc" event messages into each partitioned stream.
  
Each message is a JSON object that represents the event. It also populates 1 message into the Transfers stream that orchestrates the ETL pipeline for the given "timeId" and "useId" combination.
  
For testing we typically perform the following
1. Run the harness to create the streams. This is a one-off e.g. with the following parameters "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=createPartitionedStreams -timeId=66 -useId=22 -group=1"
2. Run the harness to publish the event messages into each partioned nats streams. For example; 
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=66 -useId=22 -group=1"
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=109 -useId=33 -group=1"
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=321 -useId=19 -group=1"
  
Note that the timeId is changing. This ensures that ...

## Deploying the fat jar and running the test harness 
In order to execute the test harness easily in a deployed env where NATS is not exposed to an external IP.
1. Execute the fatJar gradle task, this will create a jar file under build/libs/PopulateEventToNats-1.0-SNAPSHOT.jar (or something very similar)
2. Copy the jar file to a temporary directory on one of the pods that has java already available (for example ocs-aio-0)
   1. if you have kubectl configured locally to access a remote env then you can use the following command from the build/libs directory
      1. `kubectl cp -n oce-dc01  PopulateEventToNats-1.0-SNAPSHOT.jar ocs-aio-0:/tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -c main`
3. Once copied to the remote pod, log into the pod
   1. if you have the oc cli tooling installed and configured you can use something like the following command
      1. `oc -n oce-dc01 -it exec ocs-aio-0 -c main -- bash`
4. Once connect to the pod, if you are setting up the NATS env then create the streams first
   1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -tst=createPartitionedStreams -s=ocernd03-eh-nats.ocernd03-eh.svc:4222`
   2. in the above command notice the URL being used for NATS, this is using the NATS service name on the default port `ocernd03-eh-nats.ocernd03-eh.svc:4222`
5. Once the streams are created you can publish dummy messages or if you have an existing environment that already has messages in the oracle CUSTDB you can use those
6. To publish dummy messages - if you want to populate differet timeid an useid messages then you'll need to run this command multiple times
   1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -partitions=256 -s=ocernd03-eh-nats.ocernd03-eh.svc:4222 -mc=10 -tst=publishPartitionedStreams -timeId=66 -useId=22 -group=1`
7. To publish all events currently in the CUSTDB
   1. `java -Djdbc.drivers=oracle.jdbc.driver.OracleDriver -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -tst=publishFromDb -s=ocernd03-eh-nats.ocernd03-eh.svc:4222 -cdbip=10.224.60.128 -cdbport=1535`
   2. The code is assuming the default userid of adv and the password has not been changed from the default generated password.
8. If you need to reset the NATS streams , the easiest way is to first delete them and then recreate them
9. To delete all partitions
   1. `java -jar /tmp/PopulateEventToNats-1.0-SNAPSHOT.jar -tst=deletePartitionedStreams -s=ocernd03-eh-nats.ocernd03-eh.svc:4222`

