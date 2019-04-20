import org.apache.hadoop.io.ArrayWritable;

public class DocIdFreqArray extends ArrayWritable {
    public DocIdFreqArray() {
        super(DocIdFreq.class);
    }

    public DocIdFreqArray(DocIdFreq[] values) {
        super(DocIdFreq.class, values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String s : toStrings()) {
            sb.append(s);
        }
        return sb.toString();
    }
}