package org.apache.wayang.apps.parquet;

import java.util.ArrayList;

public class ParquetWorkload {

    private final String workloadName;
    private final String filePath;
    private ArrayList<String> projection;

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

    public String getFilePath() {
        return filePath;
    }

    public String getWorkloadName() {
        return workloadName;
    }
}
