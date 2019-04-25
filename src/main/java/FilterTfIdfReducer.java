import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performing a reduce join operation, the term is only written into the context if it is present on both sides, once
 * indicated through the presence of DocIdFreq objects in the array, and once having an empty array as payload.
 * KEYIN: Text - term
 * VALUEIN: Text - values from left (TfIdf-part1) and right(chi2-part2 -> empty)
 * KEYOUT: Text - term
 * VALUEOUT: Text - weight
 */
public class FilterTfIdfReducer extends Reducer<Text, DocIdFreqArray, Text, DocIdFreqArray> {

    /**
     * @param key     term
     * @param values  values
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void reduce(Text key, Iterable<DocIdFreqArray> values, Context context) throws IOException,
            InterruptedException {
        DocIdFreqArray result = null;
        boolean inMergedList = false;
        for (DocIdFreqArray arr : values) {
            if (arr.get().length == 0) {
                inMergedList = true;
            } else {
                List<DocIdFreq> l = new ArrayList<>();
                for (Writable w : arr.get()) {
                    l.add(new DocIdFreq((DocIdFreq) w));
                }
                result = new DocIdFreqArray(l.toArray(new DocIdFreq[0]));
            }
        }
        if (inMergedList && result != null) {
            context.write(key, result);
        }
    }
}

