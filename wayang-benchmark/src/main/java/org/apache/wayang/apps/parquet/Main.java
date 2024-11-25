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

import com.google.gson.JsonArray;
import org.apache.wayang.apps.parquet.workload.ExperimentStats;
import org.apache.wayang.apps.parquet.workload.RunnableWorkload;
import org.apache.wayang.apps.parquet.workload.Workload;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
            System.exit(1);
        }

        JsonArray output = new JsonArray();
        Workload.getWorkloadsFromDirectory(args[0])
                .map(RunnableWorkload::run)
                .flatMap(Arrays::stream)
                .map(ExperimentStats::getExperimentStats)
                .forEach(output::add);

        System.out.println("\nBenchmark stats:");
        System.out.println(output);
    }
}
