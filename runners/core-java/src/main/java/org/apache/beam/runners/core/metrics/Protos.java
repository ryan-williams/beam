package org.apache.beam.runners.core.metrics;

import org.apache.beam.model.jobmanagement.v1.JobApiMetrics;
import org.apache.beam.model.jobmanagement.v1.JobApiMetrics.MetricKey;
import org.apache.beam.model.jobmanagement.v1.JobApiMetrics.MetricName;
import org.apache.beam.model.pipeline.v1.PipelineMetrics.IntDistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricResult;

public class Protos {
    public static MetricKey keyFromProto(MetricResult<?> result) {
        return MetricKey.newBuilder()
                        .setStep(result.getStep())
                        .setName(
                            MetricName.newBuilder()
                                      .setNamespace(result.getName().getNamespace())
                                      .setName(result.getName().getName())
                        )
                        .build();
    }

    public static JobApiMetrics.DistributionResult fromProto(MetricResult<DistributionResult> result) {
        return JobApiMetrics.DistributionResult.newBuilder()
                                         .setAttempted(fromProto(result.getAttempted()))
                                         .setCommitted(fromProto(result.getCommitted()))
                                         .build();
    }

    public static IntDistributionData fromProto(DistributionResult distributionResult) {
        return IntDistributionData.newBuilder()
                                            .setMin(distributionResult.getMin())
                                            .setMax(distributionResult.getMax())
                                            .setCount(distributionResult.getCount())
                                            .setSum(distributionResult.getSum())
                                            .build();
    }
}
