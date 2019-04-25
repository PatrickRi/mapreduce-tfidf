import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Splits the reviewText field of JSON documents into single tokens, and additionally extracts the category for later.
 * KEYIN: Text - extracted token/term
 * VALUEIN: DocIdFreq, encapsulating documentId, frequency of the term, and the document category
 * KEYOUT: Text - token/term
 * VALUEOUT: DocIdFreqArray, list of all DocIdFreq objects belonging to the term (one per document for the respective term)
 */
public class TfIdfReducer extends Reducer<Text, DocIdFreq, Text, DocIdFreqArray> {

    public long documentCounter = 0;

    /**
     * inject counter TOTAL_DOCUMENTCOUNT into job
     *
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    /**
     * @param term    the term
     * @param values  all DocIdFreq objects belonging to the term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    public void reduce(Text term, Iterable<DocIdFreq> values, Context context) throws IOException, InterruptedException {
        ArrayList<DocIdFreq> resultSet = new ArrayList<>();
        int f_t = 0;
        // count number of documents and repack in new HashSet because cannot use iterable twice
        for (DocIdFreq val : values) {
            f_t++;
            resultSet.add(new DocIdFreq(val));
        }
        for (DocIdFreq val : resultSet) {
            double TF_B = val.frequency.get();
            double IDF_B2 = Math.log(this.documentCounter / f_t);
            val.tfidf = new DoubleWritable(TF_B * IDF_B2);
        }
        context.write(term, new DocIdFreqArray(resultSet.toArray(new DocIdFreq[resultSet.size()])));
    }

}