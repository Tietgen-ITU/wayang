package org.apache.wayang.basic.operators;

import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

public class ParquetFileSource extends UnarySource<GenericRecord> {
    private final String filepath;
    private final String[] columns;


    public ParquetFileSource(String filepath, String[] columnNames) {
        this(filepath, createOutputDataSetType(columnNames), columnNames);

    }
    private ParquetFileSource(String filepath, DataSetType<GenericRecord> type, String... columnNames) {
        super(type);
        this.filepath = filepath;
        this.columns = columnNames;
    }

    public ParquetFileSource(ParquetFileSource source) {
        this(source.filepath, source.columns);
    }

    public String getFilePath() {

        return this.filepath;
    }

    public String[] getColumns() {
        return this.columns;
    }

    private static DataSetType<GenericRecord> createOutputDataSetType(String[] columnNames) {
        return DataSetType.createDefault(GenericRecord.class);
    }
}
