package example;

import java.io.Serializable;

public class Genome implements Serializable {

    private String key;
    private String value;
    private String barcode;

    public Genome() {
    }

    public Genome(String key, String value) {
        this.key = key;
        this.value = value;
        barcode = "";
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getBarcode() { return barcode; }

    public void setBarcode(String barcode) { this.barcode = barcode; }
}
