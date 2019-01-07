package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.MetricName;

import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.USER_COUNTER_URN_PREFIX;

public class MetricUrns {
    /**
     * Parse a {@link MetricName} from a {@link
     * org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfoUrns.Enum}
     *
     * <p>Should be consistent with {@code parse_namespace_and_name} in monitoring_infos.py
     */
    public static MetricName parseUrn(String urn) {
        if (urn.startsWith(USER_COUNTER_URN_PREFIX)) {
            urn = urn.substring(USER_COUNTER_URN_PREFIX.length());
        }
        // If it is not a user counter, just use the first part of the URN, i.e. 'beam'
        String[] pieces = urn.split(":", 2);
        if (pieces.length != 2) {
            throw new IllegalArgumentException("Invalid metric URN: " + urn);
        }
        return MetricName.named(pieces[0], pieces[1]);
    }
}
