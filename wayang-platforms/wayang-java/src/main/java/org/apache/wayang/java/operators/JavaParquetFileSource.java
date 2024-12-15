package org.apache.wayang.java.operators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.wayang.basic.operators.ParquetSchema;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Here is a list of todos that we need to go through in order to fully implement this:
 * - TODO: Implement the evaluate method
 * - TODO: Figure out how to limit what is being returned
 * 
 * - NOTE: Do not implement new PlanBuilder functions in scala. I think that we can reuse the `readTable()`
 */

/**
 * Is a Java file source for Parquet files.
 * 
 * This execution operator extends the {@link ParquetFileSource} and implements
 * {@link JavaExecutionOperator}.
 */
public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {

    public JavaParquetFileSource(String inputUrl, ParquetSchema schema) {
        super(inputUrl, schema);

    }

    public JavaParquetFileSource(ParquetFileSource source) {
        super(source);
    }

    private static Schema GenerateSchema(ParquetSchema schema) {
        if (schema == null)
            return null;

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record("record")
                .fields();

        for (var column : schema.getColumns()) {
            if (column.getField1().equals("string"))
                fieldAssembler = fieldAssembler.optionalString(column.getField0());
            else if (column.getField1().equals("int"))
                fieldAssembler = fieldAssembler.optionalInt(column.getField0());
            else if (column.getField1().equals("long"))
                fieldAssembler = fieldAssembler.optionalLong(column.getField0());
            else
                throw new NotImplementedException("Type not implemented: " + column.field0);
        };

        return fieldAssembler.endRecord();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getSupportedInputChannels'");
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs,
            ChannelInstance[] outputs, JavaExecutor javaExecutor, OperatorContext operatorContext) {

        String filePath = this.getFilePath();
        String[] columns = this.getColumns();
        Configuration configuration = new Configuration();

        // Create InputFile for the parquet reader
        Path path = new Path(filePath);

        Optional<ParquetSchema> parquetSchema = this.getSchema();
        Schema schema = null;

        if (parquetSchema.isPresent()) {
            schema = GenerateSchema(parquetSchema.get());
            configuration.set(AvroReadSupport.AVRO_REQUESTED_PROJECTION, schema.toString());
        }

        Function<GenericRecord, Object[]> recordMapper = schema != null ? gr -> {
            // Prepare fields to be mapped
            Object[] recordValues = new Object[columns.length];

            // Map the fields
            for (int i = 0; i < columns.length; i++)
                recordValues[i] = gr.get(columns[i]);

            return recordValues;
        } : gr -> {
            return gr.getSchema().getFields().stream()
                    .map(x -> gr.get(x.name())).toArray();
        };

        // Open and read each record from the parquet file
        try {
            InputFile file = HadoopInputFile.fromPath(path, configuration);
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build()) {

                Stream<Record> recordStream = Stream.generate(() -> {
                    try {
                        return reader.read();
                    } catch (IOException ioException) {
                        throw new WayangException("Could not read from Parquet file.", ioException);
                    }
                }).takeWhile(Objects::nonNull).map(genericRecord -> {

                    // Return the record with the mapped fields
                    return new Record(recordMapper.apply(genericRecord));
                });

                ((StreamChannel.Instance) outputs[0]).accept(recordStream);
            }
        } catch (IOException ioException) {

            ioException.printStackTrace();
            throw new WayangException(String.format("Reading from file: %s failed.",
                    filePath), ioException);
        }

        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return mainLineageNode.collectAndMark();
    }
}
