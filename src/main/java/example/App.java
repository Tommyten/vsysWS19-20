package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Hello world!
 */
public class App {

    private static final String genomeSeqPattern = "[ACGT]{20,}";
    private static final String genomeIdPatternString = "@((.+?-){3}.+?)\\s.*";
    private static Pattern genomeIdPattern = Pattern.compile(genomeIdPatternString);

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

    private static final String barcodePrefix = "GCTTGGGTGTTTAACC";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Barcode Analysis")
                .set("spark.cassandra.connection.host", "cassandra");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Genome> genomeList = new ArrayList<>();

        JavaRDD<String> lines = context.textFile("vsysWS19-20/target/classes/test.fastq");

        String key = "";
        String value = "";
        List<String> lineList = lines.collect();

        for (String line :
                lineList) {
            Matcher matcher = genomeIdPattern.matcher(line);
            if (matcher.matches()) {
                key = matcher.group(1);
            } else if (line.matches(genomeSeqPattern)) {
                value = line;;
            }

            if (!key.isEmpty() && !value.isEmpty()) {
                genomeList.add(new Genome(key, value));
                key = "";
                value = "";
            }
        }
        for(Genome genome : genomeList) {
            determineBarcode(genome);
        }
        Map<String, String> fieldToColumnMapping = new HashMap<>();
        fieldToColumnMapping.put("key", "id");
        fieldToColumnMapping.put("barcode", "barcode");
        JavaRDD<Genome> rdd = context.parallelize(genomeList);
        javaFunctions(rdd)
                .writerBuilder("genome", "data", mapToRow(Genome.class, fieldToColumnMapping))
                .saveToCassandra();

        context.stop();
    }

    private static void determineBarcode(Genome genome) {
        if(genome == null) return;
        int[] scorePerBarcode = new int[BARCODES.length];
        for(int i = 0; i < BARCODES.length; i++) {
            String fullString = barcodePrefix + BARCODES[i]; //TODO: Test with barcodePostfix
            // Using an offset to increase certainty for a given barcode because of multiple tries
            for(int offset = 0; offset < 10; offset++) {
                scorePerBarcode[i] += levenshteinDistance(genome.getValue().substring(offset, fullString.length() + offset), fullString);
            }
        }

        int indexMinScore = 0;
        int currentMinScore = scorePerBarcode[0];
        for(int i = 0; i < scorePerBarcode.length; i++) {
            if(scorePerBarcode[i] < currentMinScore) {
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
            for(int i = 1; i < len0; i++) {
                // matching current letters in both strings
                int match = (a.charAt(i - 1) == b.charAt(j - 1)) ? 0 : 1;

                // computing cost for each transformation
                int cost_replace = cost[i - 1] + match;
                int cost_insert  = cost[i] + 1;
                int cost_delete  = newcost[i - 1] + 1;

                // keep minimum cost
                newcost[i] = Math.min(Math.min(cost_insert, cost_delete), cost_replace);
            }

            // swap cost/newcost arrays
            int[] swap = cost; cost = newcost; newcost = swap;
        }

        // the distance is the cost for transforming all letters in both strings
        return cost[len0 - 1];
    }
}
