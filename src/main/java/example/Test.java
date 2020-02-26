package example;


import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {
        String json = "{\"key\":\"test\", \"value\":\"Test\"}";
        Genome genome = new ObjectMapper().readValue(json, Genome.class);
        System.out.println(genome.toString());
    }
}
