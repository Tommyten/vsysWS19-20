package example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming {

    private static final String[] BARCODES = new String[] {
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


    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Streaming test");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<Genome> genomeStream = lines.map((Function<String, Genome>) s -> objectMapper.readValue(s, Genome.class));

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate}
    }
}