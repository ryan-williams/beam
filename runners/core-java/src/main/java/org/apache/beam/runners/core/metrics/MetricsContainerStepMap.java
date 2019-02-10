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

import static org.apache.beam.runners.core.metrics.MetricUpdatesProtos.toProto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics containers by step.
 *
 * <p>This class is not thread-safe.
 */
public class MetricsContainerStepMap implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsContainerStepMap.class);

  private Map<String, MetricsContainerImpl> metricsContainers;
  private MetricsContainerImpl unboundContainer = new MetricsContainerImpl(null);

  public MetricsContainerStepMap() {
    this.metricsContainers = new ConcurrentHashMap<>();
  }

  /* Returns the container that is not bound to any step name. */
  public MetricsContainerImpl getUnboundContainer() {
    return this.unboundContainer;
  }

  /** Returns the container for the given step name. */
  public MetricsContainerImpl getContainer(String stepName) {
    if (stepName == null) {
      // TODO(BEAM-6538): Disallow this in the future, some tests rely on an empty step name today.
      return getUnboundContainer();
    }
    if (!metricsContainers.containsKey(stepName)) {
      metricsContainers.put(stepName, new MetricsContainerImpl(stepName));
    }
    return metricsContainers.get(stepName);
  }

  /**
   * Update this {@link MetricsContainerStepMap} with all values from given {@link
   * MetricsContainerStepMap}.
   */
  public void updateAll(MetricsContainerStepMap other) {
    for (Map.Entry<String, MetricsContainerImpl> container : other.metricsContainers.entrySet()) {
      getContainer(container.getKey()).update(container.getValue());
    }
    getUnboundContainer().update(other.getUnboundContainer());
  }

  /**
   * Update {@link MetricsContainerImpl} for given step in this map with all values from given
   * {@link MetricsContainerImpl}.
   */
  public void update(String step, MetricsContainerImpl container) {
    getContainer(step).update(container);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetricsContainerStepMap that = (MetricsContainerStepMap) o;

    // TODO(BEAM-6546): The underlying MetricContainerImpls do not implement equals().
    return getMetricsContainers().equals(that.getMetricsContainers());
  }

  @Override
  public int hashCode() {
    return metricsContainers.hashCode();
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap
   * MetricsContainerStepMaps} of attempted and committed metrics.
   *
   * <p>This constructor is intended for runners which support both attempted and committed metrics.
   */
  public static MetricResults asMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers,
      MetricsContainerStepMap committedMetricsContainers) {
    return new MetricsContainerStepMapMetricResults(
        attemptedMetricsContainers, committedMetricsContainers);
  }

  /** Return the cumulative values for any metrics in this container as MonitoringInfos. */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    // Extract user metrics and store as MonitoringInfos.
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<>();
    for (MetricsContainerImpl container : getMetricsContainers()) {
      monitoringInfos.addAll(toProto(container.getUpdates()));
    }
    return monitoringInfos;
  }

  /**
   * Returns {@link MetricResults} based on given {@link MetricsContainerStepMap} of attempted
   * metrics.
   *
   * <p>This constructor is intended for runners which only support `attempted` metrics. Accessing
   * {@link MetricResult#getCommitted()} in the resulting {@link MetricResults} will result in an
   * {@link UnsupportedOperationException}.
   */
  public static MetricResults asAttemptedOnlyMetricResults(
      MetricsContainerStepMap attemptedMetricsContainers) {
    return new MetricsContainerStepMapMetricResults(attemptedMetricsContainers);
  }

  @Override
  public String toString() {
    LOG.info("Converting MCSM to MRs");
    return asAttemptedOnlyMetricResults(this).toString();
  }

  private Iterable<MetricsContainerImpl> getMetricsContainers() {
    return Iterables.concat(metricsContainers.values(), Collections.singleton(unboundContainer));
  }

  private static class MetricsContainerStepMapMetricResults extends MetricResults {
    private final Map<MetricKey, MetricResult<Long>> counters = new HashMap<>();
    private final Map<MetricKey, MetricResult<DistributionData>> distributions = new HashMap<>();
    private final Map<MetricKey, MetricResult<GaugeData>> gauges = new HashMap<>();
    private final boolean isCommittedSupported;

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers) {
      this(attemptedMetricsContainers, new MetricsContainerStepMap(), false);
    }

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers,
        MetricsContainerStepMap committedMetricsContainers) {
      this(attemptedMetricsContainers, committedMetricsContainers, true);
    }

    private MetricsContainerStepMapMetricResults(
        MetricsContainerStepMap attemptedMetricsContainers,
        MetricsContainerStepMap committedMetricsContainers,
        boolean isCommittedSupported) {
      for (MetricsContainerImpl container : attemptedMetricsContainers.getMetricsContainers()) {
        MetricUpdates cumulative = container.getCumulative();
        mergeCounters(counters, cumulative.counterUpdates(), attemptedUpdateFn());
        mergeDistributions(distributions, cumulative.distributionUpdates(), attemptedUpdateFn());
        mergeGauges(gauges, cumulative.gaugeUpdates(), attemptedUpdateFn());
      }
      for (MetricsContainerImpl container : committedMetricsContainers.getMetricsContainers()) {
        MetricUpdates cumulative = container.getCumulative();
        mergeCounters(counters, cumulative.counterUpdates(), comittedUpdateFn());
        mergeDistributions(distributions, cumulative.distributionUpdates(), comittedUpdateFn());
        mergeGauges(gauges, cumulative.gaugeUpdates(), comittedUpdateFn());
      }
      this.isCommittedSupported = isCommittedSupported;
    }

    private <T> Function<MetricUpdate<T>, MetricResult<T>> attemptedUpdateFn() {
      return input -> MetricResult.create(input.getKey(), null, input.getUpdate());
    }

    private <T> Function<MetricUpdate<T>, MetricResult<T>> comittedUpdateFn() {
      return input -> MetricResult.create(input.getKey(), input.getUpdate(), null);
    }

    @Override
    public String toString() {
      return queryMetrics(null).toString();
    }

    @Override
    public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
      return new QueryResults(filter);
    }

    private class QueryResults extends MetricQueryResults {
      private final MetricsFilter filter;

      private QueryResults(MetricsFilter filter) {
        this.filter = filter;
      }

      @Override
      public Iterable<MetricResult<Long>> getCounters() {
        return FluentIterable.from(counters.values()).filter(matchesFilter(filter)).toList();
      }

      @Override
      public Iterable<MetricResult<DistributionResult>> getDistributions() {
        return FluentIterable.from(distributions.values())
            .filter(matchesFilter(filter))
            .transform(result -> result.transform(DistributionData::extractResult))
            .toList();
      }

      @Override
      public Iterable<MetricResult<GaugeResult>> getGauges() {
        return FluentIterable.from(gauges.values())
            .filter(matchesFilter(filter))
            .transform(result -> result.transform(GaugeData::extractResult))
            .toList();
      }

      private Predicate<MetricResult<?>> matchesFilter(final MetricsFilter filter) {
        return attemptedAndCommitted ->
            MetricFiltering.matches(filter, attemptedAndCommitted.getKey());
      }
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeCounters(
        Map<MetricKey, MetricResult<Long>> counters,
        Iterable<MetricUpdate<Long>> updates,
        Function<MetricUpdate<Long>, MetricResult<Long>> updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<Long> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        MetricResult<Long> update = updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (counters.containsKey(key)) {
          MetricResult<Long> current = counters.get(key);
          update =
              MetricResult.create(
                  key,
                  update.getAttempted() + current.getAttempted(),
                  update.getCommitted() + current.getCommitted());
        }
        counters.put(key, update);
      }
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeDistributions(
        Map<MetricKey, MetricResult<DistributionData>> distributions,
        Iterable<MetricUpdate<DistributionData>> updates,
        Function<MetricUpdate<DistributionData>, MetricResult<DistributionData>>
            updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<DistributionData> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        MetricResult<DistributionData> update = updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (distributions.containsKey(key)) {
          MetricResult<DistributionData> current = distributions.get(key);
          update =
              MetricResult.create(
                  key,
                  update.getAttempted().combine(current.getAttempted()),
                  update.getCommitted().combine(current.getCommitted()));
        }
        distributions.put(key, update);
      }
    }

    @SuppressWarnings("ConstantConditions")
    private void mergeGauges(
        Map<MetricKey, MetricResult<GaugeData>> gauges,
        Iterable<MetricUpdate<GaugeData>> updates,
        Function<MetricUpdate<GaugeData>, MetricResult<GaugeData>>
            updateToAttemptedAndCommittedFn) {
      for (MetricUpdate<GaugeData> metricUpdate : updates) {
        MetricKey key = metricUpdate.getKey();
        MetricResult<GaugeData> update = updateToAttemptedAndCommittedFn.apply(metricUpdate);
        if (gauges.containsKey(key)) {
          MetricResult<GaugeData> current = gauges.get(key);
          update =
              MetricResult.create(
                  key,
                  update.getAttempted().combine(current.getAttempted()),
                  update.getCommitted().combine(current.getCommitted()));
        }
        gauges.put(key, update);
      }
    }

    /** Accumulated implementation of {@link MetricResult}. */
    private static class AccumulatedMetricResult<T> extends MetricResult<T> {
      private final MetricKey key;
      private final T attempted;
      private final @Nullable T committed;
      private final boolean isCommittedSupported;

      private AccumulatedMetricResult(
          MetricKey key, T attempted, @Nullable T committed, boolean isCommittedSupported) {
        this.key = key;
        this.attempted = attempted;
        this.committed = committed;
        this.isCommittedSupported = isCommittedSupported;
      }

      @Override
      public MetricKey getKey() {
        return key;
      }

      @Override
      public T getCommitted() {
        if (!isCommittedSupported) {
          throw new UnsupportedOperationException(
              "This runner does not currently support committed"
                  + " metrics results. Please use 'attempted' instead.");
        }
        return committed;
      }

      @Override
      public T getAttempted() {
        return attempted;
      }
    }
  }
}
