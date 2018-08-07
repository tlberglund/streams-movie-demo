package io.confluent.demo;

import static io.confluent.demo.StreamsDemo.SCHEMA_REGISTRY_URL;
import static io.confluent.demo.StreamsDemo.getMovieAvroSerde;
import static io.confluent.demo.StreamsDemo.getProperties;
import static io.confluent.demo.StreamsDemo.getRatedMoviesTable;
import static io.confluent.demo.StreamsDemo.getRatingAverageTable;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

  private static final int PARTITIONS = 1;

  private static final short REPLICAS = (short) 1;

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig streamsConfig() {
    return new StreamsConfig(getProperties(SCHEMA_REGISTRY_URL));
  }

  @Bean
  KTable ratedMoviesTable(StreamsBuilder builder) {
    KTable<Long, Double> ratingAverageTable = getRatingAverageTable(builder);
    return getRatedMoviesTable(builder, ratingAverageTable, getMovieAvroSerde(serdeConfig()));
  }

  @Bean
  Map<String, String> serdeConfig() {
    return Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                    SCHEMA_REGISTRY_URL);
  }

  @Bean
  public NewTopic ratedMovies() {
    return new NewTopic("rated-movies", PARTITIONS, REPLICAS);
  }

  @Bean
  public NewTopic movies() {
    return new NewTopic("movies", PARTITIONS, REPLICAS);
  }

  @Bean
  public NewTopic rawMovies() {
    return new NewTopic("raw-movies", PARTITIONS, REPLICAS);
  }

  @Bean
  public NewTopic rawRatings() {
    return new NewTopic("raw-ratings", PARTITIONS, REPLICAS);
  }

  @Bean
  public NewTopic averageRatings() {
    return new NewTopic("average-ratings", PARTITIONS, REPLICAS);
  }

}
