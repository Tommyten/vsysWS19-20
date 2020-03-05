package example;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URL;

public class Test {

    public static void main(String[] args) throws IOException {
        /*String json = "{\"key\":\"test\", \"value\":\"Test\"}";
        Genome genome = new ObjectMapper().readValue(json, Genome.class);
        System.out.println(genome.toString());*/
        URL nginxUrl = null;
        try {
            nginxUrl = new URL("http://my-nginx/test.fastq");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        System.out.println(nginxUrl.toString());
    }
}
