import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class Chi2DataReducer implements WritableComparable<Chi2DataReducer> {
    public Chi2DataReducer() {

    }
    public Chi2DataReducer(Text term, DoubleWritable chi2) {
        this.term = term;
        this.chi2 = chi2;
    }
    public Text term = new Text("");
    public DoubleWritable chi2 = new DoubleWritable(0d);

    @Override
    public int compareTo(Chi2DataReducer o) {
        return o.term.compareTo(this.term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeBytes(this.docId.toString());
//        dataOutput.writeLong(this.frequency.get());
//        dataOutput.writeDouble((this.tfidf.get()));
//        dataOutput.writeBytes(this.category.toString());
        this.term.write(dataOutput);
        this.chi2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.term.readFields((dataInput));
        this.chi2.readFields((dataInput));
    }
}