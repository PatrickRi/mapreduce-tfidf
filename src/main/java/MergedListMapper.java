import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Generates a tuple (term,NaN) for every term read from the merged list of top 200 terms from the chi2 calculation.
 * KEYIN: Object - index
 * VALUEIN: Text - term
 * KEYOUT: Text - term
 * VALUEOUT: DocIdFreqArray - empty - indicating its origin from the merged chi2 list
 */
public class MergedListMapper extends Mapper<Object, Text, Text, DocIdFreqArray> {

    private DocIdFreqArray empty = new DocIdFreqArray(new DocIdFreq[0]);
    private Text outputKey = new Text();

    /**
     * @param key     index
     * @param value   term
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        for(String s : value.toString().split(" ")) {
            outputKey.set(s);
            context.write(outputKey, empty);
        }

    }
}
