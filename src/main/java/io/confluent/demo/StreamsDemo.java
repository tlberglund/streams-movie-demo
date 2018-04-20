package io.confluent.demo;

import io.confluent.demo.avro.Movie;
import io.confluent.demo.avro.RatedMovie;
import io.confluent.demo.avro.Rating;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class StreamsDemo {


   public static void main(String args[]) {
      final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

      Properties streamsConfiguration = new Properties();
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-films");
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
      streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
      streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());
      streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

      final Map<String, String> serdeConfig =
         Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
         SCHEMA_REGISTRY_URL);

      final SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>();
      movieSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<Rating> ratingSerde = new SpecificAvroSerde<>();
      ratingSerde.configure(serdeConfig, false);

      final SpecificAvroSerde<RatedMovie> ratedMovieSerde = new SpecificAvroSerde<>();
      ratingSerde.configure(serdeConfig, false);


      StreamsBuilder builder = new StreamsBuilder();

      KStream<Long, String> rawRatings = builder.stream("raw-ratings",
                                                        Consumed.with(Serdes.Long(),
                                                                      Serdes.String()));
      KStream<Long, Rating> ratings = rawRatings.mapValues(Parser::parseRating)
              .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating));

      KStream<Long, Double> numericRatings = ratings.mapValues(Rating::getRating);

      KGroupedStream<Long, Double> ratingsById = numericRatings.groupByKey();

      KTable<Long, Long> ratingCounts = ratingsById.count();
      KTable<Long, Double> ratingSums = ratingsById.reduce((v1, v2) -> v1 + v2);
      KTable<Long, Double> ratingAverage =
              ratingSums.join(ratingCounts, (sum, count) -> sum / count.doubleValue());

      ratingAverage.toStream().to("average-ratings");

      KStream<Long, String> rawMovies = builder.stream("raw-movies", Consumed.with(Serdes.Long(), Serdes.String()));
      KStream<Long, Movie> movieStream = rawMovies
          .mapValues(Parser::parseMovie)
          .map((key, movie) -> new KeyValue<>(movie.getMovieId(), movie));

      movieStream.to( "movies", Produced.with(Serdes.Long(), movieSerde));

      KTable<Long, Movie> movies = builder.table("movies",
                                                 Materialized
                                                     .<Long, Movie, KeyValueStore<Bytes, byte[]>>as(
                                                         "movies-store")
                                                     .withValueSerde(movieSerde)
                                                     .withKeySerde(Serdes.Long())
      );

      KTable<Long, String> ratedMovies = ratingAverage.join(movies,
                                                            (avg, movie) -> movie.getTitle() + "=" + avg);

      ratedMovies.toStream().to("rated-movies", Produced.with(Serdes.Long(), Serdes.String()));

      KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
      Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      streams.start();
   }

}





