package io.confluent.demo;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class StreamsDemo {


   public static void main(String args[]) {
      final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

      Properties streamsConfiguration = new Properties();
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-films");
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
      streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
      //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
      streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
      streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


      final Map<String, String> serdeConfig =
         Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
         SCHEMA_REGISTRY_URL);

      final SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>();
      movieSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<Rating> ratingSerde = new SpecificAvroSerde<>();
      ratingSerde.configure(serdeConfig, false);


      KStreamBuilder builder = new KStreamBuilder();
      KStream<Long, String> rawRatings = builder.stream(Serdes.Long(), Serdes.String(), "raw-ratings");
      KStream<Long, Rating> ratings = rawRatings.mapValues(text -> Parser.parseRating(text));
      ratings.to("parsed-ratings");

                //.map((Integer key, Rating rating) -> new KeyValue<Integer, Rating>(rating.getMovieId(), rating) );
/*
        KTable<Integer, Movie> movies = builder.table("movies");
        KStream<Integer, Rating> ratings = builder.stream("ratings");

        KStream<Integer, RatedMovie> ratedMovies = ratings.join(movies,
                (Rating rating, Movie movie) -> new RatedMovie(movie.getTitle().toString(), movie.getReleaseDate(), rating.getRating()));

        ratedMovies.to("rated-movies");
*/
      KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      streams.start();
   }

}
