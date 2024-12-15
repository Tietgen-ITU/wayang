package org.apache.wayang.basic.operators;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;


public class ParquetFileSource extends UnarySource<Record> {
    private final String filepath;
    private final String[] columns;
    private final Optional<ParquetSchema> schema;

    public ParquetFileSource(String filepath, ParquetSchema schema) {
        this(filepath, createOutputDataSetType(), schema);

    }

    private ParquetFileSource(String filepath, DataSetType<Record> type, ParquetSchema schema) {
        super(type);
        this.filepath = filepath;
        this.columns = schema.getColumnNames();
        this.schema = Optional.of(schema);
    }

    public ParquetFileSource(ParquetFileSource source) {
        this(source.filepath, source.schema.get());
    }

    public String getFilePath() {

        return this.filepath;
    }

    public String[] getColumns() {

        return this.columns;
    }

    public Optional<ParquetSchema> getSchema() {

        return this.schema;
    }

    private static DataSetType<Record> createOutputDataSetType() {
        return DataSetType.createDefault(Record.class);
    }
}

