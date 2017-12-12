import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.DoubleSerializer
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.clients.producer.ProducerRecord


class RatingStreamer {

    def ratings = [
            
    ]

    static void main(args) {
        Properties props = new Properties()
        props.put('bootstrap.servers', 'localhost:9092')
        props.put('key.serializer', LongSerializer.class.getName())
        props.put('value.serializer', DoubleSerializer.class.getName())
        def producer = new KafkaProducer(props)

        long key = 1
        double rating = 1.0
        producer.send(new ProducerRecord('temp', key, rating))
        producer.flush()
        producer.close()
    }
}