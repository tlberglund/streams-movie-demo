#!/usr/bin/env bash
confluent destroy
confluent start schema-registry


cat movies.json| kafkacat -b localhost -P -t movies


cat movies.json | kafka-console-producer --broker-list localhost:9092 --topic raw-movies
cat ratings.json | kafka-console-producer --broker-list localhost:9092 --topic ratings
$CONFLUENT_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic rating-averages
$CONFLUENT_HOME/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic rated-movies
