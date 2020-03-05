package example;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomHttpReceiver extends Receiver<Genome> {

    public CustomHttpReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {

    }

    private static final String genomeSeqPattern = "[ACGT]{20,}";
    private static final String genomeIdPatternString = "@((.+?-){3}.+?)\\s.*";
    private static Pattern genomeIdPattern = Pattern.compile(genomeIdPatternString);

    private void receive() {
        URL nginxUrl = null;
        try {
            nginxUrl = new URL("my-nginx/test.fastq");
        } catch (Exception ex) {
            stop(ex.getMessage());
        }

        try (BufferedReader in = new BufferedReader(new InputStreamReader(nginxUrl.openStream()))) {
            String inputLine = in.readLine();
            String key = "";
            String value = "";
            while(!isStopped() && inputLine != null) {
                Matcher matcher = genomeIdPattern.matcher(inputLine);
                if (matcher.matches()) {
                    key = matcher.group(1);
                } else if (inputLine.matches(genomeSeqPattern)) {
                    value = inputLine;
                }

                if (!key.isEmpty() && !value.isEmpty()) {
                    System.out.println("Received Data!!!!\nKey: " + key + "\nValue: " + value + "\n");
                    store(new Genome(key, value));
                    key = "";
                    value = "";
                }
                inputLine = in.readLine();
            }
        } catch (IOException e) {
            stop(e.getMessage());

        }

    }
}
