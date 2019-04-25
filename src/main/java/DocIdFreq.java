import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates state about the origin (documentId, category of the document) and frequency of a term.
 */
public class DocIdFreq implements WritableComparable<DocIdFreq> {
    public DocIdFreq() {

    }

    public DocIdFreq(DocIdFreq o) {
        this.docId.set(o.docId);
        this.frequency.set(o.frequency.get());
        this.tfidf.set(o.tfidf.get());
        this.category.set(o.category);
    }

    public DocIdFreq(Text docId, LongWritable frequency, Text category) {
        this.docId = docId;
        this.frequency = frequency;
        this.category = category;

    }

    public Text docId = new Text("EMPTY");
    public LongWritable frequency = new LongWritable(0);
    public DoubleWritable tfidf = new DoubleWritable(0d);
    public Text category = new Text("EMPTY");

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

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("{ DocId=").append(this.docId.toString());
        result.append(", frequency=").append(this.frequency.toString());
        result.append(", tfidf=").append(this.tfidf.toString());
        result.append(", category=").append(this.category.toString());
        result.append("}");
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DocIdFreq docIdFreq = (DocIdFreq) o;
        return Objects.equals(docId, docIdFreq.docId) &&
                Objects.equals(frequency, docIdFreq.frequency) &&
                Objects.equals(tfidf, docIdFreq.tfidf) &&
                Objects.equals(category, docIdFreq.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, frequency, tfidf, category);
    }
}