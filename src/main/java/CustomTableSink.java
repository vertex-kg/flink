import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sinks.TableSink;

public class CustomTableSink /*implements TableSink*/ {

//    @Override
    public TypeInformation getOutputType() {
        return TypeInformation.of(String.class);
    }

//    @Override
    public TableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

//    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation<?>[]{TypeInformation.of(String.class)};
    }

//    @Override
    public String[] getFieldNames() {
        return new String[] { "name" };
    }
}
