import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MergeMapper extends Mapper<Text, NullWritable, Text, Text> {

    @Override
    protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(new Text("KEY"), key);
    }
}
