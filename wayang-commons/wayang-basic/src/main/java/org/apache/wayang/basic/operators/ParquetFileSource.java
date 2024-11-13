package org.apache.wayang.basic.operators;

public class ParquetFileSource extends TableSource {
    private final String filepath;

    public ParquetFileSource(String filepath, String ...columnNames) {
        super(filepath, columnNames); 
        this.filepath = filepath;
    }

    public String getFilePath() {

        return this.filepath;
    }
}
