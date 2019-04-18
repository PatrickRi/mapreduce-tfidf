import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class Chi2DataMapper implements WritableComparable<Chi2DataMapper> {
    public Text term = new Text("");
    public LongWritable A = new LongWritable(0);
    public LongWritable B = new LongWritable(0);

    public Chi2DataMapper() {

    }
    public Chi2DataMapper(Text term, LongWritable A, LongWritable B) {
        this.term = term;
        this.A = A;
        this.B = B;
    }

    @Override
    public int compareTo(Chi2DataMapper o) {
        return o.term.compareTo(this.term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeBytes(this.docId.toString());
//        dataOutput.writeLong(this.frequency.get());
//        dataOutput.writeDouble((this.tfidf.get()));
//        dataOutput.writeBytes(this.category.toString());
        this.term.write(dataOutput);
        this.A.write(dataOutput);
        this.B.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.term.readFields((dataInput));
        this.A.readFields((dataInput));
        this.B.readFields((dataInput));
    }
}