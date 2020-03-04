package example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;

public class SparkStreaming {

    private static final String[] BARCODES = new String[]{
            "AAGAAAGTTGTCGGTGTCTTTGTG",
            "TCGATTCCGTTTGTAGTCGTCTGT",
            "GAGTCTTGTGTCCCAGTTACCAGG",
            "TTCGGATTCTATCGTGTTTCCCTA",
            "CTTGTCCAGGGTTTGTGTAACCTT",
            "TTCTCGCAAAGGCAGAAAGTAGTC",
            "GTGTTACCGTGGGAATGAATCCTT",
            "TTCAGGGAACAAACCAAGTTACGT",
            "AACTAGGCACAGCGAGTCTTGGTT",
            "AAGCGTTGAAACCTTTGTCCTCTC",
            "GTTTCATCTATCGGAGGGAATGGA",
            "CAGGTAGAAAGAAGCAGAATCGGA",
            "CAGGTAGAAAGAAGCAGAATCGGA"
    };

    private static final String barcodePrefix = "GCTTGGGTGTTTAACC";

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();

        SparkConf conf = new SparkConf()
                .setMaster("spark://spark-master:7077")
                .setAppName("Barcode Analysis")
                .set("spark.cassandra.connection.host", "192.168.228.1");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "service-test:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "temperatureAlert");
        kafkaParams.put("auto.offset.reset", "earliest");

        Collection<String> topics = Arrays.asList("genome-data");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferBrokers(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<Genome> genomeStream = stream.map((Function<ConsumerRecord<String, String>, Genome>) record -> objectMapper.readValue(record.value(), Genome.class));

        JavaDStream<Genome> genomesWithBC = genomeStream.map(genome -> {
            determineBarcode(genome);
            return genome;
        });

        Map<String, String> fieldToColumnMapping = new HashMap<>();
        fieldToColumnMapping.put("key", "id");
        fieldToColumnMapping.put("barcode", "barcode");

        javaFunctions(genomesWithBC)
                .writerBuilder("genome", "data", mapToRow(Genome.class, fieldToColumnMapping))
                .saveToCassandra();

        genomesWithBC.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate}
    }

    private static void determineBarcode(Genome genome) {
        int[] scorePerBarcode = new int[BARCODES.length];
        for (int i = 0; i < BARCODES.length; i++) {
            String fullString = barcodePrefix + BARCODES[i]; //TODO: Test with barcodePostfix
            // Using an offset to increase certainty for a given barcode because of multiple tries
            for (int offset = 0; offset < 10; offset++) {
                scorePerBarcode[i] += levenshteinDistance(genome.getValue().substring(offset, fullString.length() + offset), fullString);
            }
        }

        int indexMinScore = 0;
        int currentMinScore = scorePerBarcode[0];
        for (int i = 0; i < scorePerBarcode.length; i++) {
            if (scorePerBarcode[i] < currentMinScore) {
                currentMinScore = scorePerBarcode[i];
                indexMinScore = i;
            }
        }
        genome.setBarcode(BARCODES[indexMinScore]);
    }

    // https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Java
    private static int levenshteinDistance(String a, String b) {
        int len0 = a.length() + 1;
        int len1 = b.length() + 1;

        // the array of distances
        int[] cost = new int[len0];
        int[] newcost = new int[len0];

        // initial cost of skipping prefix in String s0
        for (int i = 0; i < len0; i++) cost[i] = i;

        // dynamically computing the array of distances

        // transformation cost for each letter in s1
        for (int j = 1; j < len1; j++) {
            // initial cost of skipping prefix in String s1
            newcost[0] = j;

            // transformation cost for each letter in s0
            for (int i = 1; i < len0; i++) {
                // matching current letters in both strings
                int match = (a.charAt(i - 1) == b.charAt(j - 1)) ? 0 : 1;

                // computing cost for each transformation
                int cost_replace = cost[i - 1] + match;
                int cost_insert = cost[i] + 1;
                int cost_delete = newcost[i - 1] + 1;

                // keep minimum cost
                newcost[i] = Math.min(Math.min(cost_insert, cost_delete), cost_replace);
            }

            // swap cost/newcost arrays
            int[] swap = cost;
            cost = newcost;
            newcost = swap;
        }

        // the distance is the cost for transforming all letters in both strings
        return cost[len0 - 1];
    }
}