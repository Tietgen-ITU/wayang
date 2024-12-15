package org.apache.wayang.apps.parquet.workload;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.apps.parquet.Main;
import org.apache.wayang.basic.operators.ParquetSchema;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetFileSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class YelpWorkload extends Workload {

    public YelpWorkload(String workloadFile) {
        super(workloadFile);
    }

    @Override
    public ExperimentStats[] run() {
        String filepath = getWorkloadFilePath();
        String workloadType = getWorkloadType();

        List<ExperimentStats> experimentStats = new ArrayList<>();

        if (Objects.equals(workloadType, "parquet")) {

            ParquetSchema schema = ParquetSchema.createBuilder()
                                            .addIntColumn("label")
                                            .build();

            Collection<Experiment> experiments = IntStream.range(0, getIterations())
                    .mapToObj(x -> new Experiment(String.format("%s-yelp-0%d", workloadType, x + 1),
                            new Subject("yelp-benchmark", "v1.0")))
                    .map(x -> YelpWorkload.runParquet(new JavaParquetFileSource(filepath, null), x))
                    .collect(Collectors.toList());

            Collection<Experiment> experimentsProj = IntStream.range(0, getIterations())
                    .mapToObj(x -> new Experiment(String.format("%s-yelp-projection-0%d", workloadType, x + 1),
                            new Subject("yelp-benchmark", "v1.0")))
                    .map(x -> YelpWorkload.runParquet(new JavaParquetFileSource(filepath, schema), x))
                    .collect(Collectors.toList());

            experimentStats.add(ExperimentStats.fromCollection("yelp-parquet", filepath, experiments));
            experimentStats.add(ExperimentStats.fromCollection("yelp-parquet-projection", filepath, experimentsProj));

        } else if (Objects.equals(workloadType, "csv")) {

            Collection<Experiment> experiments = IntStream.range(0, getIterations())
                    .mapToObj(x -> new Experiment(String.format("%s-yelp-0%d", workloadType, x + 1),
                            new Subject("yelp-benchmark", "v1.0")))
                    .map(x -> YelpWorkload.runCsv(filepath, x))
                    .collect(Collectors.toList());

            experimentStats.add(ExperimentStats.fromCollection("yelp-csv", filepath, experiments));
        }

        return experimentStats.toArray(ExperimentStats[]::new);
    }

    private static Experiment runParquet(JavaParquetFileSource fileSource, Experiment experiment) {
        WayangContext context = createContext();
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Parquet Read")
                .withExperiment(experiment)
                .withUdfJarOf(Main.class);

        /* Start building the Apache WayangPlan */
        Collection<String> distinctLabels = planBuilder
                /* Read the text file */
                .readParquet(fileSource)
                .map(r -> r.getString(0))
                .distinct()
                .collect();

        return experiment;
    }

    private static Experiment runCsv(String filepath, Experiment experiment) {
        WayangContext context = createContext();
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Parquet Read")
                .withExperiment(experiment)
                .withUdfJarOf(Main.class);

        Collection<String> distinctLabels = planBuilder
                /* Read the text file */
                .readTextFile(filepath)
                .filter(row -> !row.startsWith("label")).withName("Remove headers")
                .map(x -> x.split(",")[0])
                .distinct()
                .collect();

        return experiment;
    }

    private static WayangContext createContext() {
        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        return wayangContext;
    }
}
