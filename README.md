# PopulateEventToNats
Test Harness to Publish Event History Message to Partitioned NATS Streams for ST-EH

Command line arguments for example
-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=deletePartitionedStreams -timeId=66 -useId=22 -group=1
-s is the location of the NATs URL
-mc is the number of event messages to be populated into each partition
-timeId is the timeId to use for the creation of each event. Events with the same timeId will be processed in any given interval.
-useId is the useId to use for the creation of each event.
-group is the groupdId to use for the creation of Transfer record. 
partitions is the number of partitions we have defined, usually 256
-tst is one of 
  createPartitionedStreams create a NATS stream for each of the partitions i.e. 256 partitioned streams
  deletePartitionedStreams delete the same create above.
  publishPartitionedStreams will publish "mc" event messages into each partitioned stream. Each message is a JSon object that represents the event. This will also populate 1 message into the Transfers stream that orchestrates the ETL pipeline for the given "timeId" and "useId" combination.
  
For testing we typically perform the following
1. Run the harness to create the streams. This is a one-off e.g. with the following parameters "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=createPartitionedStreams -timeId=66 -useId=22 -group=1"
2. Run the harness to publish the event messages into each partioned nats streams. For example; 
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=66 -useId=22 -group=1"
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=109 -useId=33 -group=1"
  "-partitions=256 -s=192.168.49.2:30409 -mc=200 -tst=publishPartitionedStreams -timeId=321 -useId=19 -group=1"
  
  Note that the timeId is changing. This ensures that 
