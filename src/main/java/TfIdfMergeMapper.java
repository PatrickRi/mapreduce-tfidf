import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TfIdfMergeMapper extends Mapper<Text, DocIdFreqArray, Text, Text> {

    @Override
    protected void map(Text key, DocIdFreqArray value, Context context) throws IOException, InterruptedException {
        for (Writable v : value.get()) {
            context.write(key, new Text(((DocIdFreq) v).tfidf.toString()));
        }
    }
}
