java -cp ./target/MessageProducerCompressed-1.0-SNAPSHOT-jar-with-dependencies.jar au.com.redhat.test.MessageProducerCompressed ~/Documents/Customers/Optus/jonrequest3 2000 true true 10


java -Dlog4j.debug -cp ./target/MessageProducerCompressed-1.0-SNAPSHOT-jar-with-dependencies.jar au.com.redhat.test.MessageProducerCompressed -b "failover:(tcp://192.168.1.240:61616,tcp://192.168.1.241:61616)?randomize=false&backup=true&priorityBackup=true&priorityURIs=tcp://192.168.1.240:61616&useExponentialBackOff=false&initialReconnectDelay=1000" -m 100000 -t 1 -s 500
Arguments
---------

string - filename for payload
int - message count
boolean - compresses
boolean - persistent
int - thread count
