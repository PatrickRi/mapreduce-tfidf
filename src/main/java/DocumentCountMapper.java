import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DocumentCountMapper extends Mapper<Object, Text, Text, DocIdFreq> {

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN M1");
        context.getCounter(MrChain.Count.TOTAL_DOCUMENTS).increment(1);

    }
}