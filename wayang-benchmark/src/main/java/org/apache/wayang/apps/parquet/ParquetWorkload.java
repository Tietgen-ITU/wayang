package org.apache.wayang.apps.parquet;

import java.util.ArrayList;

public class ParquetWorkload {

    private final String workloadName;
    private final String filePath;
    private ArrayList<String> projection;
    private final int iterations = 5;

    public ParquetWorkload(String name, String filePath) {

        this.workloadName = name;
        this.filePath = filePath;
    }

    public void addColumn(String column) {
        if (projection == null)
            projection = new ArrayList<>();

        projection.add(column);
    }

    public String[] getColumns() {
        return this.projection != null ?
                projection.toArray(new String[projection.size()]) :
                null;
    }

    public int getIterations() {
        return iterations;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getWorkloadName() {
        return workloadName;
    }
}
