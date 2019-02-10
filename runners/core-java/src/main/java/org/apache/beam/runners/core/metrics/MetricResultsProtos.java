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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MonitoringInfos.keyFromMonitoringInfo;
import static org.apache.beam.runners.core.metrics.MonitoringInfos.processMetric;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.metrics.DefaultMetricResults;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;

/** Convert {@link MetricResults} to and from {@link BeamFnApi.MetricResults}. */
public class MetricResultsProtos {
  private static <T> void process(
      BeamFnApi.MetricResults.Builder builder,
      MetricResult<T> metricResult,
      BiConsumer<SimpleMonitoringInfoBuilder, T> set) {
    MetricKey metricKey = metricResult.getKey();

    // Use a fresh SimpleMonitoringInfoBuilder for each possible MonitoringInfo (attempted and
    // committed)
    Supplier<SimpleMonitoringInfoBuilder> makeBuilder =
        () -> new SimpleMonitoringInfoBuilder().handleMetricKey(metricKey);

    T attempted = metricResult.getAttempted();
    if (attempted != null) {
      SimpleMonitoringInfoBuilder partial = makeBuilder.get();
      set.accept(partial, attempted);
      builder.addAttempted(partial.build());
    }

    T committed = metricResult.getCommitted();
    if (committed != null) {
      SimpleMonitoringInfoBuilder partial = makeBuilder.get();
      set.accept(partial, committed);
      builder.addCommitted(partial.build());
    }
  }

  // Convert between proto- and SDK-representations of MetricResults

  /** Convert a {@link MetricResults} to a {@linke BeamFnApi.MetricResults}. */
  public static BeamFnApi.MetricResults toProto(MetricResults metricResults) {
    BeamFnApi.MetricResults.Builder builder = BeamFnApi.MetricResults.newBuilder();
    MetricQueryResults results = metricResults.queryMetrics(MetricsFilter.builder().build());
    results
        .getCounters()
        .forEach(counter -> process(builder, counter, SimpleMonitoringInfoBuilder::setInt64Value));
    results
        .getDistributions()
        .forEach(
            distribution ->
                process(
                    builder, distribution, SimpleMonitoringInfoBuilder::setIntDistributionValue));
    results
        .getGauges()
        .forEach(gauge -> process(builder, gauge, SimpleMonitoringInfoBuilder::setGaugeValue));
    return builder.build();
  }

  private static class MutableMetricResult<T> extends MetricResult<T> {
    private final MetricKey key;
    @Nullable private T committed;
    @Nullable private T attempted;

    public <U> MutableMetricResult(MetricKey key) {
      this.key = key;
    }

    @Override
    public MetricKey getKey() {
      return key;
    }

    @Override
    public T getCommitted() {
      return committed;
    }

    @Override
    public T getAttempted() {
      return attempted;
    }

    public void setCommitted(T committed) {
      this.committed = committed;
    }

    public void setAttempted(T attempted) {
      this.attempted = attempted;
    }
  }

  /**
   * Helper for converting {@link BeamFnApi.MetricResults} to {@link MetricResults}.
   *
   * <p>The former separates "attempted" and "committed" metrics, while the latter splits on
   * metric-type (counter, distribution, or gauge) at the top level, so converting basically amounts
   * to performing that pivot.
   */
  private static class PTransformMetricResultsBuilder {
    private final Map<MetricKey, MutableMetricResult<Long>> counters = new ConcurrentHashMap<>();
    private final Map<MetricKey, MutableMetricResult<DistributionResult>> distributions =
        new ConcurrentHashMap<>();
    private final Map<MetricKey, MutableMetricResult<GaugeResult>> gauges =
        new ConcurrentHashMap<>();

    public PTransformMetricResultsBuilder(BeamFnApi.MetricResults metrics) {
      add(metrics.getAttemptedList(), false);
      add(metrics.getCommittedList(), true);
    }

    public Map<MetricKey, MetricResult<Long>> getCounters() {
      return (Map) counters;
    }

    public Map<MetricKey, MetricResult<DistributionResult>> getDistributions() {
      return (Map) distributions;
    }

    public MetricResults build() {
      return new DefaultMetricResults(
          getCounters().values(), getDistributions().values(), getGauges().values());
    }

    public Map<MetricKey, MetricResult<GaugeResult>> getGauges() {
      return (Map) gauges;
    }

    public void add(Iterable<MonitoringInfo> monitoringInfos, Boolean committed) {
      for (MonitoringInfo monitoringInfo : monitoringInfos) {
        add(monitoringInfo, committed);
      }
    }

    public void add(MonitoringInfo monitoringInfo, Boolean committed) {
      add(keyFromMonitoringInfo(monitoringInfo), monitoringInfo.getMetric(), committed);
    }

    public void add(MetricKey metricKey, Metric metric, Boolean committed) {
      processMetric(
          metric,
          counter -> add(metricKey, counter, committed, counters),
          distribution -> add(metricKey, distribution, committed, distributions),
          gauge -> add(metricKey, gauge, committed, gauges));
    }

    public <T> void add(
        MetricKey key, T value, Boolean committed, Map<MetricKey, MutableMetricResult<T>> map) {
      MutableMetricResult<T> result = new MutableMetricResult<>(key);
      result = MoreObjects.firstNonNull(map.putIfAbsent(key, result), result);
      if (committed) {
        result.setCommitted(value);
      } else {
        result.setAttempted(value);
      }
    }
  }

  public static MetricResults fromProto(BeamFnApi.MetricResults metricResults) {
    return new PTransformMetricResultsBuilder(metricResults).build();
  }
}
