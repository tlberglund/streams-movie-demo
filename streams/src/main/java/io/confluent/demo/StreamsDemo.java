package io.confluent.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
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

  public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

  public static void main(String args[]) {

    Properties streamsConfiguration = getProperties(SCHEMA_REGISTRY_URL);

    final Map<String, String> serdeConfig =
        Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                 SCHEMA_REGISTRY_URL);

    final SpecificAvroSerde<Movie> movieSerde = getMovieAvroSerde(serdeConfig);

    final SpecificAvroSerde<Rating> ratingSerde = getRatingAvroSerde(serdeConfig);

    final SpecificAvroSerde<RatedMovie> ratedMovieSerde = new SpecificAvroSerde<>();
    ratingSerde.configure(serdeConfig, false);

    StreamsBuilder builder = new StreamsBuilder();

    KTable<Long, Double> ratingAverage = getRatingAverageTable(builder);

    getRatedMoviesTable(builder, ratingAverage, movieSerde);

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    streams.start();
  }

  private static SpecificAvroSerde<Rating> getRatingAvroSerde(Map<String, String> serdeConfig) {
    final SpecificAvroSerde<Rating> ratingSerde = new SpecificAvroSerde<>();
    ratingSerde.configure(serdeConfig, false);
    return ratingSerde;
  }

  public static SpecificAvroSerde<Movie> getMovieAvroSerde(Map<String, String> serdeConfig) {
    final SpecificAvroSerde<Movie> movieSerde = new SpecificAvroSerde<>();
    movieSerde.configure(serdeConfig, false);
    return movieSerde;
  }

  public static KTable<Long, String> getRatedMoviesTable(StreamsBuilder builder,
                                                         KTable<Long, Double> ratingAverage,
                                                         SpecificAvroSerde<Movie> movieSerde) {

    builder.stream("raw-movies", Consumed.with(Serdes.Long(), Serdes.String()))
        .mapValues(Parser::parseMovie)
        .map((key, movie) -> new KeyValue<>(movie.getMovieId(), movie))
        .to("movies", Produced.with(Serdes.Long(), movieSerde));

    KTable<Long, Movie> movies = builder.table("movies",
                                               Materialized
                                                   .<Long, Movie, KeyValueStore<Bytes, byte[]>>as(
                                                       "movies-store")
                                                   .withValueSerde(movieSerde)
                                                   .withKeySerde(Serdes.Long())
    );

    KTable<Long, String> ratedMovies = ratingAverage
        .join(movies, (avg, movie) -> movie.getTitle() + "=" + avg);

    ratedMovies.toStream().to("rated-movies", Produced.with(Serdes.Long(), Serdes.String()));
    return ratedMovies;
  }

  public static KTable<Long, Double> getRatingAverageTable(StreamsBuilder builder) {
    KStream<Long, String> rawRatings = builder.stream("raw-ratings",
                                                      Consumed.with(Serdes.Long(),
                                                                    Serdes.String()));
    KStream<Long, Rating> ratings = rawRatings.mapValues(Parser::parseRating)
        .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating));

    KStream<Long, Double> numericRatings = ratings.mapValues(Rating::getRating);

    KGroupedStream<Long, Double> ratingsById = numericRatings.groupByKey();

    KTable<Long, Long> ratingCounts = ratingsById.count();
    KTable<Long, Double> ratingSums = ratingsById.reduce((v1, v2) -> v1 + v2);

    KTable<Long, Double> ratingAverage = ratingSums.join(ratingCounts,
                                                         (sum, count) -> sum / count.doubleValue(),
                                                         Materialized.as("average-ratings"));
    ratingAverage.toStream()
        /*.peek((key, value) -> { // debug only
          System.out.println("key = " + key + ", value = " + value);
        })*/
        .to("average-ratings");
    return ratingAverage;
  }

  public static Properties getProperties(String SCHEMA_REGISTRY_URL) {
    Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-films");
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfiguration
        .put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    streamsConfiguration
        .put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Enable record cache of size 10 MB.
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
    // Set commit interval to 1 second.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    return streamsConfiguration;
  }

}





