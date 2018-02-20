## Kafka Streams and KSQL Demo 

This repo is a storehouse of bits of code useful for demonstrating Kafka Streams and Confluent KSQL. By way of expectation management, it's not a coherent demo project as such, but it does have useful things in it that you might want to know how to use.

### Building

The main Gradle build file names all the dependencies needed to build the project. The build file doesn't contain any tasks to run the demo, but is useful as an illustration

Running `gradlew idea` will generate IntelliJ IDEA project files. It is from there that I typically run `StreamsDemo.java`, which has a `main()` method containing a simple Kafka Streams application.

### Test Data

The `data` directory contains two strangely-formatted data files, `movies.dat` and `ratings.dat`. These are parsed by `io.confluent.demo.Parser` into Avro objects called `io.confluent.demo.Movie` and `io.confluent.demo.Rating`. This all happens for free in `StreamsDemo.java`.

That directory also contains JSON versions of the data called `movies-json.js` and `ratings-json.js`. These are handy for demonstrating Streams and KSQL operating on JSON data.
