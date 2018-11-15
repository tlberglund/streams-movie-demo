package io.confluent.demo;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertNotNull;

@Slf4j
public class MoviesTopologyTest {


  public static String
      LETHAL_WEAPON_MOVIE =
      "122::Lethal Weapon 2::1989::110::United States::6.0::48092::Action::Mel Gibson|Danny Glover|Joe Pesci|Patsy Kensit|Joss Ackland|Derrick O'Connor|Traci Wolfe|Darlene Love|Steve Kahan::Richard Donner::Michael Kamen|Eric Clapton|David Sanborn::Jeffrey Boam::Stephen Goldblatt::Warner Bros";

  TopologyTestDriver td;

  @Before
  public void setUp() {
    final Properties
        streamsConfig =
        StreamsDemo.getStreamsConfig("dummy.kafka.confluent.cloud:9092",
                                     "dummy.sr.confluent.cloud:8080");

    StreamsBuilder builder = new StreamsBuilder();
    StreamsDemo.getMoviesTable(builder);

    final Topology topology = builder.build();
    log.info("Topology: \n{}", topology.describe());
    td = new TopologyTestDriver(topology, streamsConfig);
  }

  @Test
  public void validateIfTestDriverCreated() {
    assertNotNull(td);
  }

  @Test
  public void validateAvroMovie() {
    ConsumerRecordFactory<Long, String> rawMovieRecordFactory =
        new ConsumerRecordFactory<>(StreamsDemo.RAW_MOVIES_TOPIC_NAME, new LongSerializer(),
                                    new StringSerializer());

    td.pipeInput(rawMovieRecordFactory.create(LETHAL_WEAPON_MOVIE));

    //td.readOutput("")
    final KeyValueStore<Long, Movie> movieStore =
        td.getKeyValueStore("movies-store");
    final Movie movie = movieStore.get(122L);
    Assert.assertEquals("Lethal Weapon 2", movie.getTitle());

  }
}
