import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class DocIdFreqArray extends ArrayWritable {
    public DocIdFreqArray() {
        super(DocIdFreq.class);
    }

    public DocIdFreqArray(DocIdFreq[] values) {
        super(DocIdFreq.class, values);
    }

//    @Override
//    public DocIdFreq[] get() {
////        if (true)
////            throw new RuntimeException((super.get().toString() + "###" + super.get().getClass().toString()));
//        return super.get();
//    }

//    @Override
//    public String toString() {
//        DocIdFreq[] values = get();
//        return values[0].docId.toString() + ", " + values[0].frequency.toString() + ", " + values[0].tfidf.toString() + ", " + values[0].category.toString();
//    }
}