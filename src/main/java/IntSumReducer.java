import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class IntSumReducer extends Reducer<Text, DocIdFreq, Text, DocIdFreqArray> {

    public long documentCounter = 0;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    public void reduce(Text term, Iterable<DocIdFreq> values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN R2 term=" + term.toString());
//            log.warning("ERR Counter=" + documentCounter);

        //TF_B= r(dt)=f(dt)
        //IDF_B2 = loge (N/ft)
        //TF_B * IDF_B2
        // fdt = number of term t in document d
        // N = number of documents
        // ft = number of documents with term t
        ArrayList<DocIdFreq> resultSet = new ArrayList<>();
        int f_t = 0;
        // count number of documents and repack in new HashSet because cannot use iterable twice
        for (DocIdFreq val : values) {
            f_t++;
            resultSet.add(val);
        }
//        System.out.println("OUT Counter=" + this.documentCounter);
//        if (true)
//            throw new RuntimeException("Counter=" + documentCounter);
        for(DocIdFreq val: resultSet) {
            double TF_B = val.frequency.get();
            double IDF_B2 = Math.log(this.documentCounter / f_t);
            val.tfidf = new DoubleWritable(TF_B * IDF_B2);
            // throw new RuntimeException("Reducer Exception " + key.toString());
//            resultArray[i] = val;
            System.out.println("CUSTOM IN R2 value=" + val.toString());
        }
        context.write(term, new DocIdFreqArray(resultSet.toArray(new DocIdFreq[resultSet.size()])));
    }


    // iterate over terms and calculate TFIDF
    // TF_B = t_f,d
//    HashMap<String, Long> termFrequency = new HashMap<>();
//    Iterator it = terms.entrySet().iterator();
//        while (it.hasNext()) {
//        Map.Entry<String, DocIdFreq> pair = (Map.Entry)it.next();
//        if (termFrequency.containsKey(pair.getKey())) {
//            long t_df = termFrequency.get(pair.getKey()).longValue();
//            termFrequency.put(pair.getKey(), Long.valueOf(t_df + pair.getValue().frequency.get()));
//        } else {
//            termFrequency.put(pair.getKey(), Long.valueOf(pair.getValue().frequency.get()));
//        }
//    }
//    it = terms.entrySet().iterator();
//        while (it.hasNext()) {
//        Map.Entry<String, DocIdFreq> pair = (Map.Entry)it.next();
//        long tf_b = pair.getValue().frequency.get();
//
//        double idf_b = Math.log(1 + count / );
//    }
}