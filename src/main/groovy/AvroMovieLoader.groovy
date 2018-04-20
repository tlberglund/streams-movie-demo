import io.confluent.demo.Parser
import io.confluent.demo.Movie
import io.confluent.demo.avro.Movie
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.DoubleSerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer


// Nasty little hack to generate random ratings for fun movies
class AvroMovieLoader {

   static void main(args) {

      Properties props = new Properties()
      props.put('bootstrap.servers', args[0])
      props.put('key.serializer', 'org.apache.kafka.common.serialization.LongSerializer')
      props.put('value.serializer', 'io.confluent.kafka.serializers.KafkaAvroSerializer')
      props.put('schema.registry.url', 'http://localhost:8081')
      KafkaProducer producer = new KafkaProducer(props)

      try {
         long currentTime = System.currentTimeSeconds()
         println currentTime
         
         println args[0]
         def movieFile = new File(args[0])
         movieFile.eachLine { line ->
           Movie movie = Parser.parseMovie(line)
           def pr = new ProducerRecord('movies-raw', movie.movieId, movie)
           producer.send(pr)
         }
      }
      finally {
         producer.close()
      }
   }
}
