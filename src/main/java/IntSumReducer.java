import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class IntSumReducer extends Reducer<Text, DocIdFreq, Text, DocIdFreq> {

    public long documentCounter = 0;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    public void reduce(Text key, Iterable<DocIdFreq> values, Context context) throws IOException, InterruptedException {
//            log.warning("ERR Counter=" + documentCounter);

        //TF_B= r(dt)=f(dt)
        //IDF_B2 = loge (N/ft)
        //TF_B * IDF_B2
        // fdt = number of term t in document d
        // N = number of documents
        // ft = number of documents with term t
        HashSet<DocIdFreq> newSet = new HashSet<DocIdFreq>();
        int f_t = 0;
        for (DocIdFreq val : values) {
            f_t++;
            newSet.add(val);
        }
        System.out.println("OUT Counter=" + documentCounter);
//        if (true)
//            throw new RuntimeException("Counter=" + documentCounter);

        for(DocIdFreq val: newSet) {
            double TF_B = val.frequency.get();
            double IDF_B2 = Math.log(documentCounter / f_t);
            val.tfidf = new DoubleWritable(TF_B * IDF_B2);
            context.write(key, val);
//                throw new RuntimeException("Reducer Exception " + key.toString());
        }
    }
}