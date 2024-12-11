package org.apache.wayang.basic.operators;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

public class ParquetFileSource extends UnarySource<Record> {
    private final String filepath;
    private final String[] columns;
    private final Optional<Schema> schema;

    public ParquetFileSource(String filepath, String[] columnNames) {
        this(filepath, createOutputDataSetType(), columnNames);

    }

    private ParquetFileSource(String filepath, DataSetType<Record> type, String... columnNames) {
        super(type);
        System.out.println(columnNames);
        this.filepath = filepath;
        this.columns = columnNames;

        this.schema = GenerateSchema(this.columns);
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

    public Optional<Schema> getSchema() {

        return this.schema;
    }

    private static Optional<Schema> GenerateSchema(String... columnNames) {

        if (columnNames == null || columnNames.length == 0)
            return Optional.empty();

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record("record")
                .fields();

        for (String column : columnNames)
            // TODO: Implement adding the correct types...
            // fieldAssembler = fieldAssembler.optionalInt(column);
            fieldAssembler = fieldAssembler.optionalLong(column);

        return Optional.of(fieldAssembler.endRecord());
    }

    private static DataSetType<Record> createOutputDataSetType() {
        return DataSetType.createDefault(Record.class);
    }
}
