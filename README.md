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
  
Note that the timeId is changing. This ensures that 
