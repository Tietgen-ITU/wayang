/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetFileSource;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
            System.exit(1);
        }

        ParquetWorkload[] experiments = getExperiments(args[0]);

        Arrays.stream(experiments).forEach(Main::run);
    }

    private static void run(ParquetWorkload workload) {

        try {

            System.out.println("Running parquet workload with " + workload.getWorkloadName());

            Experiment exp = new Experiment("parquet-bench-yelp", new Subject("parquet-benchmark", "v1.0"));
            long startTime = System.currentTimeMillis();

            WayangContext wayangContext = new WayangContext();
            wayangContext.register(Java.basicPlugin());

            /* Get a plan builder */
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("Parquet Read")
                    .withExperiment(exp)
                    .withUdfJarOf(Main.class);

            /* Start building the Apache WayangPlan */
            Collection<String> wordcounts = planBuilder
                    /* Read the text file */
                    .readParquet(new JavaParquetFileSource(workload.getFilePath(), workload.getColumns()))
                    .map(r -> r.getString(0))
                    .distinct()
                    .collect();

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.printf("Total elapsed time(latency) %d ms%n", elapsedTime);

            printExperiment(exp);
            System.out.println();

            System.out.printf("Found %d words:\n", wordcounts.size());
            System.out.println(wordcounts);

        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

    private static ParquetWorkload[] getExperiments(String filepath) {

        ParquetWorkload[] workloads = new ParquetWorkload[2];

        workloads[0] = new ParquetWorkload("No projection", filepath);
        workloads[1] = new ParquetWorkload("Projection", filepath);

        workloads[1].addColumn("label");

        return workloads;
    }

    private static void printExperiment(Experiment experiment) {
        System.out.println("\nMeasurements:");
        for (Measurement m : experiment.getMeasurements()) {
            if (m instanceof TimeMeasurement) {
                System.out.println("Time measurement: " + m);
            }
        }
    }
}
