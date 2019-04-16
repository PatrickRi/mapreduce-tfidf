import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class DocIdFreq implements WritableComparable<DocIdFreq> {
    public DocIdFreq() {

    }
    public DocIdFreq(Text docId, LongWritable frequency, Text category) {
        this.docId = docId;
        this.frequency = frequency;
        this.category = category;
    }
    public Text docId = new Text("");
    public LongWritable frequency = new LongWritable(0);
    public DoubleWritable tfidf = new DoubleWritable(0d);
    public Text category = new Text("");

    @Override
    public int compareTo(DocIdFreq o) {
        return o.docId.compareTo(this.docId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.docId.write(dataOutput);
        this.frequency.write(dataOutput);
        this.tfidf.write(dataOutput);
        this.category.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.docId.readFields((dataInput));
        this.frequency.readFields((dataInput));
        this.tfidf.readFields((dataInput));
        this.category.readFields((dataInput));
    }
}