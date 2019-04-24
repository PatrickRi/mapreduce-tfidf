import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Increments the counter TOTAL_DOCUMENTS by one for every line=document
 * KEYIN: Object - index
 * VALUEIN: Text - line of text a.k.a. one document
 * KEYOUT: Text - not used
 * VALUEOUT: DocIdFreq - not used
 */
public class DocumentCountMapper extends Mapper<Object, Text, Text, DocIdFreq> {

    /**
     * @param key     - index
     * @param value   - line of text a.k.a. one document
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter(MrChain.Count.TOTAL_DOCUMENTS).increment(1);
    }

}