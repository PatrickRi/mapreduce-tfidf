import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Groups the DocIdFreqArrays by category, for calculating the chiSquare for categories.
 * KEYIN: Text - token/term
 * VALUEIN: DocIdFreqArray - all DocIdFreq objects belonging to the term
 * KEYOUT: category
 * VALUEOUT: Chi2DataArray, list of Chi2Data objects
 */
public class Chi2Mapper extends Mapper<Text, DocIdFreqArray, Text, Chi2DataArray> {

    /**
     * As the formula for chi2 states 4(5) parameters, A, B, C, D we calculate whats possible at this point of time.
     *
     * @param term    term
     * @param values  DocIdFreqArray
     * @param context context
     * @throws IOException          ex
     * @throws InterruptedException ex
     */
    @Override
    public void map(Text term, DocIdFreqArray values, Context context) throws IOException, InterruptedException {
        // A
        HashMap<Text, HashMap<Text, Chi2Data>> categories = new HashMap<>();
        for (Writable val : values.get()) {
            DocIdFreq docIdFreq = (DocIdFreq) val;
            if (categories.containsKey(docIdFreq.category)) {
                HashMap<Text, Chi2Data> categoryData = categories.get(docIdFreq.category);
                if (categoryData.containsKey(term)) {
                    categoryData.get(term).A.set(categoryData.get(term).A.get() + 1);// number of documents containing t in c
                } else {
                    categoryData.put(term, new Chi2Data(term, 1L, 0L, 0L, 0L));
                }
            } else {
                HashMap<Text, Chi2Data> terms = new HashMap<>();
                terms.put(term, new Chi2Data(term, 1, 0, 0L, 0L));
                categories.put(docIdFreq.category, terms);
            }
        }
        // B
        for (Map.Entry<Text, HashMap<Text, Chi2Data>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2Data> currentTerm : category.getValue().entrySet()) {
                currentTerm.getValue().B.set(this.countDocumentsWithTermNotInCategory(categories, currentTerm.getKey(), category.getKey()));
            }
        }
        // D
        for (Map.Entry<Text, HashMap<Text, Chi2Data>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2Data> currentTerm : category.getValue().entrySet()) {
                currentTerm.getValue().D.set(currentTerm.getValue().A.get() + currentTerm.getValue().B.get() + currentTerm.getValue().C.get());
            }
        }

        for (Map.Entry<Text, HashMap<Text, Chi2Data>> category : categories.entrySet()) {
            ArrayList<Chi2Data> resultList = new ArrayList<>();
            for (Text term1 : category.getValue().keySet()) {
                resultList.add(category.getValue().get(term1));
            }
            context.write(category.getKey(), new Chi2DataArray(resultList.toArray(new Chi2Data[0])));
        }
    }

    private long countDocumentsWithTermNotInCategory(HashMap<Text, HashMap<Text, Chi2Data>> categories, Text term, Text category) {
        long count = 0;
        for (Text categoryKey : categories.keySet()) {
            if (!category.toString().equals(categories.toString())) { // all categories except current
                HashMap<Text, Chi2Data> categoryData = categories.get(categoryKey);
                if (categoryData.containsKey(term)) { // find term
                    count += categoryData.get(term).A.get(); // number of documents containing t but not in c
                }
            }
        }
        return count;
    }
}