import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashSet;

public class MrChain {


    public enum Count {
        TOTAL_DOCUMENTS
    }

    public static void main(String[] args) throws Exception{
        //Job-Dependencies can be controlled:
        //http://timepasstechies.com/job-chaining-mapreduce-jobcontrol-controlledjob-driver/

// PHASE 1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MrCHAIN-COUNT-DOCUMENTS");
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
// PHASE 2
        conf = new Configuration();
        conf.set("TOTAL_DOCUMENTCOUNT", Long.toString(count)); // inject counter value into job
        job = Job.getInstance(conf, "MrCHAIN-TFIDF");
        job.setNumReduceTasks(2);
        job.setJarByClass(MrChain.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocIdFreq.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DocIdFreqArray.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/2"));

        boolean phase2succeeded = job.waitForCompletion(true);
        if (!phase2succeeded) {
            throw new IllegalStateException("Job failed!");
        }
// PHASE 3
        conf = new Configuration();
        conf.set("TOTAL_DOCUMENTCOUNT", Long.toString(count)); // inject counter value into job
        job = Job.getInstance(conf, "MrCHAIN-CHI2");
        job.setNumReduceTasks(2);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(MrChain.class);
        job.setMapperClass(Chi2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Chi2DataMapperArray.class);
        job.setReducerClass(Chi2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //https://stackoverflow.com/questions/20094366/hadoop-provide-directory-as-input-to-mapreduce-job
//        FileInputFormat.addInputPath(job, new Path(args[1]+"/2/part-r-00000"));
//        FileInputFormat.addInputPath(job, new Path(args[1]+"/2/part-r-00001"));
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[1]+"/2"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/3"));
        boolean phase3succeeded = job.waitForCompletion(true);
        if (!phase3succeeded) {
            throw new IllegalStateException("Job failed!");
        }
        //TODO merge lists
        conf = new Configuration();
        job = Job.getInstance(conf, "MrCHAIN-MERGE");
        job.setNumReduceTasks(1);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(MrChain.class);
        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[1]+"/3"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/4"));
        boolean phase4succeeded = job.waitForCompletion(true);
        if (!phase4succeeded) {
            throw new IllegalStateException("Job failed!");
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
