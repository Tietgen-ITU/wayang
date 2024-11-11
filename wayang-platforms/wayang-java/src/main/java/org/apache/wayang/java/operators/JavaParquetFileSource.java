package org.apache.wayang.java.operators;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.wayang.core.api.exception.WayangException;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.optimizer.OptimizationContext.OperatorContext;
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
 */

/**
 * Is a Java file source for Parquet files.
 * 
 * This execution operator extends the {@link ParquetFileSource} and implements
 * {@link JavaExecutionOperator}.
 */
public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {

    private static final Logger logger = LoggerFactory.getLogger(JavaParquetFileSource.class);
    private final String[] columnNames;

    public JavaParquetFileSource(String inputUrl, String ...columnNames) {
        super(inputUrl, columnNames);

        this.columnNames = columnNames;
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

        // Create InputFile for the parquet reader
        Path path = Path.of(filePath);
        InputFile file = new LocalInputFile(path);

        // Open and read each record from the parquet file
        try {
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build()) {

                // Create a iterator of records based on the parquet reader
                Iterator<GenericRecord> iterator = new ParquetIterator(reader); 

                // Create a stream of records from the iterator
                Stream<Record> recordStream = StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .map(genericRecord -> {

                        // Prepare fields to be mapped
                        Object[] recordValues = new Object[columnNames.length];

                        // Map the fields
                        for (int i = 0; i < columnNames.length; i++)
                            recordValues[i] = genericRecord.get(columnNames[i]);

                        // Return the record with the mapped fields
                        return new Record(recordValues);
                    });

                ((StreamChannel.Instance) outputs[0]).accept(recordStream);
            }
        } catch (IOException ioException) {

            ioException.printStackTrace();
            throw new WayangException(String.format("Reading from file: %s failed.", filePath), ioException);
        }

        return null; // TODO: Look at the text file source and do the same...
    }

    private class ParquetIterator implements Iterator<GenericRecord> {

        private final ParquetReader<GenericRecord> reader;
        private GenericRecord nextRecord;

        public ParquetIterator(ParquetReader<GenericRecord> reader) {

            this.reader = reader;
            this.nextRecord = this.readNext();
        }

        private GenericRecord readNext() {

            try {

                return this.reader.read();
            } catch (IOException ioException) {

                throw new WayangException("Could not read from Parquet file.", ioException);
            }
        }

        @Override
        public boolean hasNext() {

            return this.nextRecord != null;
        }

        @Override
        public GenericRecord next() {

            GenericRecord current = this.nextRecord;
            this.nextRecord = this.readNext();

            return current;
        }
    }
}
