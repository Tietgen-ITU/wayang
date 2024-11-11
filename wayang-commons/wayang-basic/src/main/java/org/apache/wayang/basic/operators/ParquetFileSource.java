package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.basic.types.RecordType;

public class ParquetFileSource extends UnarySource<Record> {
    private final String filepath;

    public ParquetFileSource(String filepath, String ...columnNames) {
       this(filepath, createOutputDataSetType(columnNames)); 
    }

    private ParquetFileSource(String filepath, DataSetType<Record> type) {
        super(type);
        this.filepath = filepath;
    }

    public String getFilePath() {

        return this.filepath;
    }

    private static DataSetType<Record> createOutputDataSetType(String[] columnNames) {
        return columnNames.length == 0 ?
                DataSetType.createDefault(Record.class) :
                DataSetType.createDefault(new RecordType(columnNames));
    }
}
