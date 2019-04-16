import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MrChain {


    public enum Count {
        TOTAL_DOCUMENTS
    }

    public static void main(String[] args) throws Exception{
//        long docCount = countTotalDocuments(input, 7);
//        return calculateTfIdf(input, 7, docCount);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MrCHAIN1");
        job.setNumReduceTasks(2);
        job.setJarByClass(MrChain.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(DocumentCountMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/1"));

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }
        long count = job.getCounters().findCounter(Count.TOTAL_DOCUMENTS).getValue();

        conf = new Configuration();
        conf.set("TOTAL_DOCUMENTCOUNT", Long.toString(count));
        job = Job.getInstance(conf, "MrCHAIN2");
        job.setNumReduceTasks(2);
        job.setJarByClass(MrChain.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocIdFreq.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
