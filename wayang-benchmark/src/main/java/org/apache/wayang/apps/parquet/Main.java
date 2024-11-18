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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }

            System.out.println("This is the file input");
            System.out.println(args[0]);

            WayangContext wayangContext = new WayangContext();
            wayangContext.register(Java.basicPlugin());


            /* Get a plan builder */
            JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("Parquet Read")
                    .withUdfJarOf(Main.class);

            /* Start building the Apache WayangPlan */
            Collection<GenericRecord> wordcounts = planBuilder
                    /* Read the text file */
                    .readParquet(new JavaParquetFileSource(args[0], "label", "text"))
                    .collect();

            System.out.println(wordcounts);


            System.out.printf("Found %d words:\n", wordcounts.size());
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }
}

