import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class Chi2Reducer extends Reducer<Text, Chi2DataMapperArray, Text, Text> {

    public long documentCounter = 0;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    public void reduce(Text key, Chi2DataMapperArray values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN R3 " + key.toString());
        // calc C
        // calc D = N -(A+B+C)
    }

}