package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class ParquetSchema {
    private final Tuple2<String, String>[] columns;

    public ParquetSchema(Tuple2<String, String>... columns) {
        this.columns = columns;
    }

    public String[] getColumnNames() {
        return Arrays.stream(this.columns)
                .map(Tuple2::getField0)
                .toArray(String[]::new);
    }

    public Tuple2<String, String>[] getColumns() {
        return this.columns;
    }

    public static SchemaBuilder createBuilder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {

        private final List<Tuple2<String, String>> columns = new ArrayList<>();

        public SchemaBuilder() {
        }

        public SchemaBuilder addStringColumn(String name) {
            this.columns.add(new Tuple2<>(name, "string"));
            return this;
        }

        public SchemaBuilder addLongColumn(String name) {
            this.columns.add(new Tuple2<>(name, "long"));
            return this;
        }

        public SchemaBuilder addIntColumn(String name) {
            this.columns.add(new Tuple2<>(name, "int"));
            return this;
        }

        public ParquetSchema build() {
            return new ParquetSchema(this.columns.toArray(Tuple2[]::new));
        }
    }
}
