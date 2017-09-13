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
      streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
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


      KStream<Long, String> rawRatings = builder.stream("raw-ratings");

      //rawRatings.print();

      KStream<Long, Rating> ratings = rawRatings
              .mapValues(text -> Parser.parseRating(text))
              .map((key, rating) -> new KeyValue<Long, Rating>(rating.getMovieId(), rating));

      //ratings.print();

      KStream<Long, String> rawMovies = builder.stream("raw-movies");
      KStream<Long, Movie> movieStream = rawMovies
              .mapValues(text -> Parser.parseMovie(text))
              .map((key, movie) -> new KeyValue<Long, Movie>(movie.getMovieId(), movie));

      movieStream.to(Serdes.Long(), movieSerde,"movies");

      //movieStream.print();

      KTable<Long, Movie> movies = builder.table(Serdes.Long(), movieSerde, "movies", "movie-store");

      KStream<Long, RatedMovie> ratedMovies = ratings.join(movies,
              (rating, movie) -> new RatedMovie(movie.getMovieId(), movie.getTitle().toString(), movie.getReleaseDate(), rating.getRating()),
              Serdes.Long(),
              ratingSerde);
//      ratedMovies.print();
/*
      KTable<Long, Float> longCounts = ratings.countByKey();

      // In another ktable, sum the values on a ten second tumbling window.
      KTable<Windowed<String>, Long> longSums =
              longs.reduceByKey((v1, v2) -> v1 + v2,
                      TimeWindows.of("longSums", 10000L),
                      Serdes.String(),
                      Serdes.Long());

      // We can join the two tables to get the average.
      KTable<Windowed<String>, Double> longAvgs =
              longSums.join(longCounts,
                      (sum, count) ->
                              sum.doubleValue()/count.doubleValue());

*/


      KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      streams.start();
   }

}
