package org.apache.wayang.apps.parquet.workload;

import java.io.File;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class Workload implements RunnableWorkload {

    private final String workloadFilePath;
    private final String workloadType;
    private final int iterations = 1;

    public Workload(String workloadFilePath) {
        this.workloadFilePath = workloadFilePath;
        this.workloadType = readWorkloadType(workloadFilePath);
    }

    public String getWorkloadFilePath() {
        return workloadFilePath;
    }

    public int getIterations() {
        return iterations;
    }

    public String getWorkloadType() {
        return workloadType;
    }

    private String readWorkloadType(String file) {
        return file.endsWith(".parquet") ? "parquet" : "csv";
    }

    private static final String[] workloadTypes = new String[] { "ssb", "yelp" };

    public static Stream<RunnableWorkload> getWorkloadsFromDirectory(String directory) {

        return Stream.of(workloadTypes).flatMap(workloadType -> getFilesFromDirectory(directory + "/" + workloadType)
                .map(filePath -> createWorkloads(workloadType, filePath))
                .filter(Objects::nonNull));
    }

    private static Stream<String> getFilesFromDirectory(String directoryPath) {

        File directory = new File(directoryPath);

        File[] files = directory.listFiles();

        if (files == null)
            return Stream.empty(); /* We do not want to throw because
                                      then we would have to handle a bunch of stupid try catch...
                                      I am too old for that stuff! */

        return Stream.of(files).map(x -> "file://" + x.toString());
    }

    private static RunnableWorkload createWorkloads(String workloadType, String file) {

        switch (workloadType) {
            case "ssb":
                return new SSBWorkload(file);
            case "yelp":
                return new YelpWorkload(file);
            default:
                return null;
        }
    }
}
