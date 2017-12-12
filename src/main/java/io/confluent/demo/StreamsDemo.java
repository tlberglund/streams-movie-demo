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
      streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
      streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Float().getClass().getName());
      streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      final Map<String, String> serdeConfig =
         Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
         SCHEMA_REGISTRY_URL);

      final SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>();
      movieSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<Rating> ratingSerde = new SpecificAvroSerde<>();
      ratingSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<RatedMovie> ratedMovieSerde = new SpecificAvroSerde<>();
      ratingSerde.configure(serdeConfig, false);


      KStreamBuilder builder = new KStreamBuilder();

      KStream<Long, String> rawRatings = builder.stream(Serdes.Long(), Serdes.String(),"raw-ratings");
      KStream<Long, Rating> ratings = rawRatings
              .mapValues(Parser::parseRating)
              .map((key, rating) -> new KeyValue(rating.getMovieId(), rating));

      KStream<Long, Float> numericalRatings = ratings.mapValues(rating -> rating.getRating());
      KGroupedStream<Long, Float> ratingsByMovieId = numericalRatings.groupByKey();
      KTable<Long, Long> ratingCount = ratingsByMovieId.count();
      KTable<Long, Float> ratingSum = ratingsByMovieId.reduce((r1, r2) -> r1 + r2);
      KTable<Long, Float> ratingAvg = ratingSum.join(ratingCount,
              (sum, count) -> sum.floatValue() / count.floatValue());

      ratingAvg.to(Serdes.Long(), Serdes.Float(), "rating-averages");

      KStream<Long, String> rawMovies = builder.stream(Serdes.Long(), Serdes.String(), "raw-movies");
      KStream<Long, Movie> movieStream = rawMovies
              .mapValues(Parser::parseMovie)
              .map((key, movie) -> new KeyValue(movie.getMovieId(), movie));

      movieStream.to(Serdes.Long(), movieSerde, "movies");

      KTable<Long, Movie> movies = builder.table(Serdes.Long(), movieSerde, "movies", "movie-store");

      KTable<Long, String> ratedMovies = ratingAvg.join(movies,
              (avg, movie) -> movie.getTitle() + "=" + avg);

      ratedMovies.to(Serdes.Long(), Serdes.String(), "rated-movies");
      ratedMovies.toStream().print();

      KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      streams.start();
   }

}
