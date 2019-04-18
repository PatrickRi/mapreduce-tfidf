import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Chi2Mapper extends Mapper<Text, DocIdFreqArray, Text, Chi2DataMapperArray> {

    public class Chi2AB {
        public long A = 0l;
        public long B = 0l;
        public long C = 0l;
        public long D = 0l;

        public Chi2AB() {

        }
        public Chi2AB(long A, long B, long C, long D) {
            this.A = A;
            this.B = B;
            this.C = C;
            this.D = D;
        }
    }


    public void map(Text term, DocIdFreqArray values, Context context) throws IOException, InterruptedException {
        System.out.println("CUSTOM IN M3A Key=" + term.toString() + ":::" + values.get().length);
        // A
        HashMap<Text, HashMap<Text, Chi2AB>> categories = new HashMap<>();
        for (Writable val : values.get()) {
            DocIdFreq docIdFreq = (DocIdFreq) val;
            if (categories.containsKey(docIdFreq.category)) {
                HashMap<Text, Chi2AB> categoryData = categories.get(docIdFreq.category);
                if (categoryData.containsKey(term)) {
                    categoryData.get(term).A++; // number of documents containing t in c
                } else {
                    categoryData.put(term, new Chi2AB(1l, 0l, 0l, 0l));
                }
            } else {
                HashMap<Text, Chi2AB> terms = new HashMap<>();
                terms.put(term, new Chi2AB(1, 0, 0l, 0l));
                categories.put(docIdFreq.category, terms);
            }
        }
        // B
        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2AB> currentTerm : category.getValue().entrySet()) {
                currentTerm.getValue().B = this.countDocumentsWithTermNotInCategory(categories, currentTerm.getKey(), category.getKey());
            }
        }
        // C
        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2AB> currentTerm : category.getValue().entrySet()) {
                long C = this.countDocumentsWithoutTermInCategory(category.getValue(), currentTerm.getKey());
            }
        }
        // D
        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category : categories.entrySet()) {
            for (Map.Entry<Text, Chi2AB> currentTerm : category.getValue().entrySet()) {
                currentTerm.getValue().D = (currentTerm.getValue().A + currentTerm.getValue().B + currentTerm.getValue().C);
            }
        }

        System.out.println("CUSTOM IN M3B");
//        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()){
//            StringBuffer terms = new StringBuffer();
//            for (Text termString : category.getValue().keySet()) {
//                terms.append(termString.toString());
//                terms.append(",");
//            }
//            new DocIdFreqArray(resultSet.toArray(new DocIdFreq[resultSet.size()]))
//      }

        for (Map.Entry<Text, HashMap<Text, Chi2AB>> category: categories.entrySet()) {
            ArrayList<Chi2DataMapper> resultList = new ArrayList<>();
            for (Text term1 : category.getValue().keySet()) {
                resultList.add(new Chi2DataMapper(term1, category.getValue().get(term1).A, category.getValue().get(term1).B, category.getValue().get(term1).C, category.getValue().get(term1).D));
            }
            System.out.println("CUSTOM IN M3C" + category.getKey().toString() + ":::" + resultList.size());
            context.write(category.getKey(), new Chi2DataMapperArray(resultList.toArray(new Chi2DataMapper[resultList.size()])));
        }
        System.out.println("CUSTOM IN M3D");
    }

    private long countDocumentsWithTermNotInCategory(HashMap<Text, HashMap<Text, Chi2AB>> categories, Text term, Text category) {
        long count = 0;
        for ( Text categoryKey: categories.keySet()) {
            if (!category.toString().equals(categories.toString())) { // all categories except current
                HashMap<Text, Chi2AB> categoryData = categories.get(categoryKey);
                if (categoryData.containsKey(term)) { // find term
                    count += categoryData.get(term).A; // number of documents containing t but not in c
                }
            }
        }
        return count;
    }
    private long countDocumentsWithoutTermInCategory(HashMap<Text, Chi2AB> terms, Text term) {
        long count = 0l;
        for (Map.Entry<Text, Chi2AB> current : terms.entrySet()) {
            if (!current.getKey().toString().equals(term.toString())) {
                count += current.getValue().A;
            }
        }
        return count;
    }
}