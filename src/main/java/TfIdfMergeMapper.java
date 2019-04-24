import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Forwards the read TfIdf values and terms from part 1 to the reducer, for it to merge.
 * KEYIN: Text - term
 * VALUEIN: DocIdFreqArray - all DocIdFreq objects belonging to the term
 * KEYOUT: Text - term
 * VALUEOUT: Text - tfidf score
 */
public class TfIdfMergeMapper extends Mapper<Text, DocIdFreqArray, Text, Text> {

    private Text outputValue = new Text();

    /**
     * @param key     term
     * @param value   array of DocIdFreq objects
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Text key, DocIdFreqArray value, Context context) throws IOException, InterruptedException {
        for (Writable v : value.get()) {
            outputValue.set(((DocIdFreq) v).tfidf.toString());
            context.write(key, outputValue);
        }
    }
}
