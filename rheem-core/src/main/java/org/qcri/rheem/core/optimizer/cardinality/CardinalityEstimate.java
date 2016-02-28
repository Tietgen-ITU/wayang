package org.qcri.rheem.core.optimizer.cardinality;

import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;

/**
 * An estimate of cardinality within a {@link RheemPlan} expressed as a {@link ProbabilisticIntervalEstimate}.
 */
public class CardinalityEstimate extends ProbabilisticIntervalEstimate {

    public CardinalityEstimate(long lowerEstimate, long upperEstimate, double correctnessProb) {
        super(lowerEstimate, upperEstimate, correctnessProb);
    }
}