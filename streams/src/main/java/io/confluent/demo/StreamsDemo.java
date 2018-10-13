package io.confluent.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import static org.apache.kafka.streams.StreamsConfig.*;

public class StreamsDemo {

  private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
  private static final String KAFKA_BOOTSTRAP_SERVER = "localhost:9092";

  public static void main(String args[]) {

    Properties config = getStreamsConfig(KAFKA_BOOTSTRAP_SERVER, SCHEMA_REGISTRY_URL);

    final Map<String, String> serdeConfig =
        Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                                 SCHEMA_REGISTRY_URL);

    final SpecificAvroSerde<Movie> movieSerde = getMovieAvroSerde(serdeConfig);
    final SpecificAvroSerde<Rating> ratingSerde = getRatingAvroSerde(serdeConfig);
    final SpecificAvroSerde<RatedMovie> ratedMovieSerde = new SpecificAvroSerde<>();
    ratingSerde.configure(serdeConfig, false);

    // Starting creating topology
    StreamsBuilder builder = new StreamsBuilder();

    // Ratings processor
    KStream<Long, String> rawRatingsStream = builder.stream("raw-ratings",
                                                            Consumed.with(Serdes.Long(),
                                                                          Serdes.String()));
    KTable<Long, Double> ratingAverage = getRatingAverageTable(rawRatingsStream);

    // Movies processors
    final KStream<Long, String> rawMovies = builder.stream("raw-movies",
                                                           Consumed.with(Serdes.Long(),
                                                                         Serdes.String()));

    getRatedMoviesTable(rawMovies, ratingAverage, movieSerde);

    // finish the topology
    Topology topology = builder.build();
    KafkaStreams streamsApp = new KafkaStreams(topology, config);

    Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
    streamsApp.start();
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

  public static KTable<Long, String> getRatedMoviesTable(KStream<Long, String> rawMovies,
                                                         KTable<Long, Double> ratingAverage,
                                                         SpecificAvroSerde<Movie> movieSerde) {

    rawMovies
        .mapValues(Parser::parseMovie)
        .map((key, movie) -> new KeyValue<>(movie.getMovieId(), movie))
        .to("movies", Produced.with(Serdes.Long(), movieSerde));

    /*KTable<Long, Movie> movies = builder.table("movies",
                                               Materialized
                                                   .<Long, Movie, KeyValueStore<Bytes, byte[]>>as(
                                                       "movies-store")
                                                   .withValueSerde(movieSerde)
                                                   .withKeySerde(Serdes.Long())*/
    );

    ValueJoiner<Double, Movie, String> joiner = (avg, movie) -> movie.getTitle() + "=" + avg;
    KTable<Long, String> ratedMovies = ratingAverage.join(movies, joiner);

    ratedMovies.toStream().to("rated-movies", Produced.with(Serdes.Long(), Serdes.String()));
    return ratedMovies;
  }

  

  public static KTable<Long, Double> getRatingAverageTable(KStream<Long, String> rawRatings) {

    KStream<Long, Rating> ratings = rawRatings.mapValues(Parser::parseRating)
        .map((key, rating) -> new KeyValue<>(rating.getMovieId(), rating));

    // Parsing Ratings
    KStream<Long, Double> numericRatings = ratings.mapValues(Rating::getRating);

    KGroupedStream<Long, Double> ratingsById = numericRatings.groupByKey();

    KTable<Long, Long> ratingCounts = ratingsById.count();
    KTable<Long, Double> ratingSums = ratingsById.reduce((v1, v2) -> v1 + v2);

    KTable<Long, Double> ratingAverage = ratingSums.join(ratingCounts,
                                                         (sum, count) -> sum / count.doubleValue(),
                                                         Materialized.as("average-ratings"));
    ratingAverage.toStream()
        .peek((key, value) -> { // debug only
          System.out.println("key = " + key + ", value = " + value);
        })
        .to("average-ratings");
    return ratingAverage;
  }

  private static Properties getStreamsConfig(String kafkaBootStrapServer,
                                             String schemaRegistryUrl) {
    Properties config = new Properties();
    config.put(APPLICATION_ID_CONFIG, "kafka-films");
    config.put(BOOTSTRAP_SERVERS_CONFIG, kafkaBootStrapServer);
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());
    // start from the beginning
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // Enable record cache of size 10 MB.
    config.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
    // Set commit interval to 1 second.
    config.put(COMMIT_INTERVAL_MS_CONFIG, 1000);

    return config;
  }

}





