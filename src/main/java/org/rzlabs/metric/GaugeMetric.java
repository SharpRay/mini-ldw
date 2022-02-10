package org.rzlabs.metric;

/**
 * Gauge metric is updated every time it is visited
 */
public abstract class GaugeMetric<T> extends Metric<T> {

    public GaugeMetric(String name, MetricUnit unit, String description) {
        super(name, MetricType.GAUGE, unit, description);
    }
}
