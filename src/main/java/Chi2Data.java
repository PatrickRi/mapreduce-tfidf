import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Chi2Data implements WritableComparable<Chi2Data> {
    public Text term = new Text("");
    public LongWritable A = new LongWritable(0);
    public LongWritable B = new LongWritable(0);
    public LongWritable C = new LongWritable(0);
    public LongWritable D = new LongWritable(0);

    public Chi2Data() {

    }

    public Chi2Data(Chi2Data o) {
        this.term.set(o.term);
        this.A.set(o.A.get());
        this.B.set(o.B.get());
        this.C.set(o.C.get());
        this.D.set(o.D.get());
    }

    public Chi2Data(Text term, LongWritable A, LongWritable B) {
        this.term = term;
        this.A = A;
        this.B = B;
        this.C = C;
        this.D = D;
    }

    public Chi2Data(Text term, long A, long B, long C, long D) {
        this.term = new Text(term);
        this.A = new LongWritable(A);
        this.B = new LongWritable(B);
        this.C = new LongWritable(C);
        this.D = new LongWritable(D);
    }

    @Override
    public int compareTo(Chi2Data o) {
        return o.term.compareTo(this.term);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
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