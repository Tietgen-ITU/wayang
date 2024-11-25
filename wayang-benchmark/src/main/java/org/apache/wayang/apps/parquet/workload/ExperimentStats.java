package org.apache.wayang.apps.parquet.workload;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.profiling.CostMeasurement;
import org.apache.wayang.core.profiling.PartialExecutionMeasurement;

import java.util.*;

public class ExperimentStats {

    private final JsonObject experimentStats;

    private ExperimentStats(String name, JsonObject[] measurements) {
        JsonObject parent = new JsonObject();

        parent.addProperty("name", name);

        JsonArray experiments = new JsonArray();
        for(JsonObject j : measurements)
            experiments.add(j);

        parent.add("measurements", experiments);


        experimentStats = parent;
    }

    public JsonObject getExperimentStats() {
        return experimentStats;
    }

    public static ExperimentStats fromArray(String name, Experiment... experiments) {

        return fromCollection(name, Arrays.asList(experiments));
    }

    public static ExperimentStats fromCollection(String name, Collection<Experiment> experiments) {

        JsonObject[] measurements = experiments.stream()
                        .map(Experiment::getMeasurements)
                        .map(xs -> {
                            JsonObject jsonObject = new JsonObject();
                            xs.forEach(x -> addMeasurement(jsonObject, x));
                            return jsonObject;
                        }).toArray(JsonObject[]::new);

        return new ExperimentStats(name, measurements);
    }

    private static void addMeasurement(JsonObject jsonObj, Measurement measurement) {
        if (measurement instanceof TimeMeasurement) {
            TimeMeasurement timeMeasurement = (TimeMeasurement) measurement;

            jsonObj.addProperty(timeMeasurement.getId(), timeMeasurement.getMillis());
        } else if (measurement instanceof CostMeasurement) {
            CostMeasurement cm = (CostMeasurement) measurement;

            jsonObj.addProperty(cm.getId() + "-upper", cm.getUpperCost());
            jsonObj.addProperty(cm.getId() + "-lower", cm.getLowerCost());

        } else if (measurement instanceof PartialExecutionMeasurement) {
            PartialExecutionMeasurement em = (PartialExecutionMeasurement) measurement;

            jsonObj.addProperty(em.getId(), em.getExecutionMillis());
            jsonObj.addProperty(em.getId() + "-upper-estimate", em.getEstimatedExecutionMillis().getUpperEstimate());
            jsonObj.addProperty(em.getId() + "-lower-estimate", em.getEstimatedExecutionMillis().getLowerEstimate());
        }
    }
}
