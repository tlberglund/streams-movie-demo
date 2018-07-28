package io.confluent.demo;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import static io.confluent.demo.StreamsDemo.*;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public StreamsConfig streamsConfig() {
    return new StreamsConfig(getProperties(SCHEMA_REGISTRY_URL));
  }


  @Bean
  KTable ratedMovies(StreamsBuilder builder) {
    KTable<Long, Double> ratingAverageTable = getRatingAverageTable(builder);
    return getRatedMoviesTable(builder, ratingAverageTable, getMovieAvroSerde(serdeConfig()));
  }

  @Bean
  Map<String, String> serdeConfig() {
    return Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                    SCHEMA_REGISTRY_URL);
  }

}
