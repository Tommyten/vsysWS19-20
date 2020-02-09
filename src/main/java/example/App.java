package example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
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
    static Pattern genomeIdPattern = Pattern.compile(genomeIdPatternString);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ReadTest");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Genome> genomeList = new ArrayList<>();

        JavaRDD<String> lines = context.textFile("testProject/target/classes/test.fastq");

        String key = "";
        String value = "";
        boolean takeNext = false;

        List<String> lineList = lines.collect();

        for (String line :
                lineList) {
            Matcher matcher = genomeIdPattern.matcher(line);
            System.out.println("In for loop");
            if (matcher.matches()) {
                key = matcher.group(1);
                System.out.println("\n\n\nFound genomeID: " + key + "\n\n\n");
            } else if (line.matches(genomeSeqPattern)) {
                value = line;
                System.out.println("\n\n\nFound genome: " + value + "\n\n\n");
            }

            if (!key.isEmpty() && !value.isEmpty()) {
                genomeList.add(new Genome(key, value));
                key = "";
                value = "";
                System.out.println("\n\n\nAdded genome!\n\n\n");
            }
        }

        System.out.println("\n\n\n\nLIST LENGTH: " + genomeList.size() + "\n\n\n");
        JavaRDD<Genome> rdd = context.parallelize(genomeList);
        javaFunctions(rdd).writerBuilder("genomes", "kv", mapToRow(Genome.class)).saveToCassandra();
    }
}
