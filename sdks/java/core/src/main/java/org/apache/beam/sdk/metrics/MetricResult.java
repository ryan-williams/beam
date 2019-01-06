package org.apache.beam.sdk.metrics;

public interface MetricResult<T> {
  public abstract MetricName getName();

  public abstract String getStep();

  public abstract T getCommitted();

  public abstract T getAttempted();

  public static <T> DefaultMetricResult<T> create(
      MetricName name, String step, T committed, T attempted) {
    return new AutoValue_DefaultMetricResult<>(name, step, committed, attempted);
  }
}
