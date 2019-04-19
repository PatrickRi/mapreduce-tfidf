import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class Chi2Reducer extends Reducer<Text, Chi2DataMapperArray, Text, NullWritable> {

    public long documentCounter = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    public void reduce(Text category, Chi2DataMapperArray values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN R3A " + category.toString());
        for (Writable val : values.get()) {
            Chi2DataMapper dataMapper = (Chi2DataMapper) val;
            System.out.println("CUSTOM IN R3B " + dataMapper.term.toString() + ":A=" + dataMapper.A.toString() + ":B=" + dataMapper.B.toString());
        }
        PriorityQueue<Chi2ValuePair> queue = new PriorityQueue<>();
        for (Writable val : values.get()) {
            Chi2DataMapper dataMapper = (Chi2DataMapper) val;
            // calc C (sum(c) - A)
            dataMapper.C.set(values.get().length - dataMapper.A.get());
            // calc D = N -(A+B+C)
            dataMapper.D.set(documentCounter - dataMapper.A.get() - dataMapper.B.get() - dataMapper.C.get());
            queue.add(new Chi2ValuePair(calculateChi2(dataMapper.A.get(), dataMapper.B.get(), dataMapper.C.get(), dataMapper.D.get()), dataMapper));
        }
        //get top 200
        for (int i = 0; i < 200 && !queue.isEmpty(); i++) {
            //only write key here, because we only need the terms, nothing else
            //https://stackoverflow.com/questions/23601380/hadoop-mapreduce-how-to-store-only-values-in-hdfs
            context.write(queue.poll().data.term, NullWritable.get());
        }
    }

    private double calculateChi2(long A, long B, long C, long D) {
        double divisor = documentCounter * Math.pow(A * D - B * C, 2);
        double divider = (A + B) * (A + C) * (B + D) * (C + D);
        return divisor / divider;
    }

    private static class Chi2ValuePair implements Comparable {
        public double chi2;
        public Chi2DataMapper data;

        public Chi2ValuePair(double chi2, Chi2DataMapper data) {
            this.chi2 = chi2;
            this.data = data;
        }

        @Override
        public int compareTo(Object o) {
            //reverse sorting, because PriorityQueue is sorting this way, and we want the biggest ones
            return Double.compare(((Chi2ValuePair) o).chi2, this.chi2);
        }
    }
}