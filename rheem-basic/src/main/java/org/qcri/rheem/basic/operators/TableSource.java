package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.StringUtils;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Stream;

/**
 * {@link UnarySource} that provides the tuples from a database table.
 */
public class TableSource extends UnarySource<Record> {

    private final String tableName;
    protected static String delimiter = ",";

    public TableSource(){tableName="";};

    public String getTableName() {
        return tableName;
    }

    /**
     * Creates a new instance.
     *
     * @param tableName   name of the table to be read
     * @param columnNames names of the columns in the tables; can be omitted but allows to inject schema information
     *                    into Rheem, so as to allow specific optimizations
     */
    public TableSource(String tableName, String... colNames) {
        this(tableName, createOutputDataSetType(colNames));
        delimiter = ",";
    }

    public TableSource(String tableName, String deli) {
        this(tableName,getColumnNames(tableName));
        delimiter = deli;
    }

    public TableSource(String tableName,String d, String... colNames)  {
        this(tableName, createOutputDataSetType(colNames));
        delimiter = d;
    }

    public TableSource(String tableName, DataSetType<Record> type) {
        super(type);
        this.tableName = tableName;

    }

    public void setDelimiter(String d){
        delimiter = d;
    }

    public String getDelimiter(){
        return delimiter;
    }


    public static String[] getColumnNames(String url){
        String myHeader = null;
        FileSystem fs = FileSystems.getFileSystem(url).orElseThrow(
                () -> new RheemException(String.format("Cannot access file system of %s.", url))
        );

        try {
            final InputStream inputStream = fs.open(url);
            Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
            myHeader = lines.findFirst().get();
        } catch (IOException e) {
            throw new RheemException(String.format("Reading %s failed.", url), e);
        }

        String columnNames[] = StringUtils.split(myHeader,delimiter);

        return columnNames;
    }


    public String getInputUrl() {
        return tableName; // in case of csv files we assume file name is the full path of a csv file
    }

    public RecordType getSchema() {
        return (RecordType)this.getType().getDataUnitType();
    }

    /**
     * Constructs an appropriate output {@link DataSetType} for the given column names.
     *
     * @param columnNames the column names or an empty array if unknown
     * @return the output {@link DataSetType}, which will be based upon a {@link RecordType} unless no {@code columnNames}
     * is empty
     */
    private static DataSetType<Record> createOutputDataSetType(String[] columnNames) {
        return columnNames.length == 0 ?
                DataSetType.createDefault(Record.class) :
                DataSetType.createDefault(new RecordType(columnNames));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TableSource(TableSource that) {
        super(that);
        this.tableName = that.getTableName();
    }

}
