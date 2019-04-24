import org.apache.hadoop.io.ArrayWritable;

public class Chi2DataArray extends ArrayWritable {
    public Chi2DataArray() {
        super(Chi2Data.class);
    }

    public Chi2DataArray(Chi2Data[] values) {
        super(Chi2Data.class, values);
    }

}