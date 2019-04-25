import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Output1Data implements WritableComparable<Output1Data> {
    public Text term = new Text("");
    public DoubleWritable tfidf = new DoubleWritable(0);

    public Output1Data() {

    }

    @Override
    public int compareTo(Output1Data o) {
        return term.compareTo(o.term);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        term.write(out);
        tfidf.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        tfidf.readFields(in);
    }
}
