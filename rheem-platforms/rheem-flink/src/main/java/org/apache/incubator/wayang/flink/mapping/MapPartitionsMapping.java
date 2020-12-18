package org.apache.incubator.wayang.flink.mapping;

import org.apache.incubator.wayang.basic.operators.MapPartitionsOperator;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.mapping.OperatorPattern;
import org.apache.incubator.wayang.core.mapping.PlanTransformation;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanPattern;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.flink.operators.FlinkMapPartitionsOperator;
import org.apache.incubator.wayang.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link MapPartitionsOperator} to {@link FlinkMapPartitionsOperator}.
 */
@SuppressWarnings("unchecked")
public class MapPartitionsMapping implements Mapping{

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singletonList(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        FlinkPlatform.getInstance()
                )
        );
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "mapPartitions", new MapPartitionsOperator<>(null, DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<MapPartitionsOperator>(
                (matchedOperator, epoch) -> new FlinkMapPartitionsOperator<>(matchedOperator).at(epoch)
        );
    }
}