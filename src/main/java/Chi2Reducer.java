import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Calculates the Chi2 score for the terms per category
 * KEYIN: Text - category
 * VALUEIN: Chi2DataArray - all Arrays belonging to the category
 * KEYOUT: Text - category
 * VALUEOUT: Text - top 200 scored terms per category, separated by a whitespace
 */
public class Chi2Reducer extends Reducer<Text, Chi2DataArray, Text, Text> {

    private long documentCounter = 0;
    private Text outputValue = new Text();

    /**
     * Inject counter TOTAL_DOCUMENTCOUNT into reducer
     *
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.documentCounter = conf.getLong("TOTAL_DOCUMENTCOUNT", -1);
    }

    /**
     * Calculates the chi2 values for every term for the given category, and only returns the top 200 ones.
     *
     * @param category        Text - category
     * @param valuesIterablee - all Arrays belonging to the category
     * @param context         context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    protected void reduce(Text category, Iterable<Chi2DataArray> valuesIterablee, Context context) throws IOException, InterruptedException {
        List<Chi2Data> values = new ArrayList<>();
        for (Chi2DataArray arr : valuesIterablee) {
            for (Writable val : arr.get()) {
                values.add((Chi2Data) val);
            }
        }
        //Use PriorityQueue for obtaining the top 200 terms using natural sorting of the used Chi2Data objects
        PriorityQueue<Chi2ValuePair> queue = new PriorityQueue<>();
        for (Writable val : values) {
            Chi2Data chi2Data = (Chi2Data) val;
            // calc C (sum(c) - A)
            chi2Data.C.set(values.size() - chi2Data.A.get());
            // calc D = N -(A+B+C)
            chi2Data.D.set(documentCounter - chi2Data.A.get() - chi2Data.B.get() - chi2Data.C.get());
            queue.add(new Chi2ValuePair(calculateChi2(chi2Data.A.get(), chi2Data.B.get(), chi2Data.C.get(), chi2Data.D.get()), chi2Data));
        }
        StringBuilder sb = new StringBuilder();
        //get top 200
        for (int i = 0; i < 200 && !queue.isEmpty(); i++) {
            sb.append(queue.poll().data.term);
            sb.append(" ");
            //only write key here, because we only need the terms, nothing else
            //https://stackoverflow.com/questions/23601380/hadoop-mapreduce-how-to-store-only-values-in-hdfs
        }
        outputValue.set(sb.toString());
        context.write(category, outputValue);
    }

    private double calculateChi2(long A, long B, long C, long D) {
        double divisor = documentCounter * Math.pow(A * D - B * C, 2);
        double divider = (A + B) * (A + C) * (B + D) * (C + D);
        //Check for division by 0
        if (divider != 0) {
            return divisor / divider;
        } else {
            return 0.0;
        }
    }

    /**
     * Value class (POJO) being naturally comparable by its chi2 score implementing Comparable
     */
    private static class Chi2ValuePair implements Comparable {
        public double chi2;
        public Chi2Data data;

        public Chi2ValuePair(double chi2, Chi2Data data) {
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