import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Forwards the TfIdf values and terms from part 1 to the reducer, for it to merge.
 * KEYIN: Text - term
 * VALUEIN: DocIdFreqArray - all DocIdFreq objects belonging to the term
 * KEYOUT: Text - term
 * VALUEOUT: DocIdFreqArray - array
 */
public class TfIdfMergeMapper extends Mapper<Text, DocIdFreqArray, Text, DocIdFreqArray> {

    /**
     * @param key     term
     * @param value   array of DocIdFreq objects
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Text key, DocIdFreqArray value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
