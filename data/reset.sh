confluent destroy
confluent start schema-registry
cat movies.dat| kafka-console-producer --broker-list localhost:9092 --topic raw-movies
cat ratings.dat | kafka-console-producer --broker-list localhost:9092 --topic raw-ratings
$CONFLUENT_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic movies
