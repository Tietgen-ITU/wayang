package org.apache.wayang.java.mapping;

import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.java.operators.JavaParquetFileSource;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ParquetFileSourceMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                JavaPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "source", new ParquetFileSource((String)null, (String[]) null), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<ParquetFileSource>(
                (matchedOperator, epoch) -> new JavaParquetFileSource(matchedOperator).at(epoch)
        );
    }
}
