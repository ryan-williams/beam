package org.apache.beam.runners.core.metrics;

import org.apache.beam.model.jobmanagement.v1.JobApiMetrics;
import org.apache.beam.model.jobmanagement.v1.JobApiMetrics.MetricKey;
import org.apache.beam.model.pipeline.v1.PipelineMetrics.IntDistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricResult;

public class Protos {

    public static MetricName toProto(JobApiMetrics.MetricName name) {
        return MetricName.named(name.getNamespace(), name.getName());
    }

    public static MetricKey keyFromProto(MetricResult<?> result) {
        return MetricKey.newBuilder()
                        .setStep(result.getStep())
                        .setName(
                            JobApiMetrics.MetricName.newBuilder()
                                      .setNamespace(result.getName().getNamespace())
                                      .setName(result.getName().getName())
                        )
                        .build();
    }

    public static JobApiMetrics.DistributionResult distributionToProto(MetricResult<DistributionResult> result) {
        JobApiMetrics.DistributionResult.Builder builder =
            JobApiMetrics.DistributionResult.newBuilder()
                                            .setAttempted(toProto(result.getAttempted()));
        try {
            builder.setCommitted(toProto(result.getCommitted()));
        } catch (UnsupportedOperationException ignored) {}

        return builder.build();
    }

    public static IntDistributionData toProto(DistributionResult distributionResult) {
        return IntDistributionData.newBuilder()
                                            .setMin(distributionResult.getMin())
                                            .setMax(distributionResult.getMax())
                                            .setCount(distributionResult.getCount())
                                            .setSum(distributionResult.getSum())
                                            .build();
    }

    public static DistributionResult fromProto(IntDistributionData distributionData) {
        return DistributionResult.create(
            distributionData.getSum(),
            distributionData.getCount(),
            distributionData.getMin(),
            distributionData.getMax()
        );
    }

    public static JobApiMetrics.CounterResult counterToProto(MetricResult<Long> result) {
        JobApiMetrics.CounterResult.Builder builder =
            JobApiMetrics.CounterResult.newBuilder().setAttempted(result.getAttempted());
        try {
            builder.setCommitted(result.getCommitted());
        } catch (UnsupportedOperationException ignored) {}

        return builder.build();
    }
}
