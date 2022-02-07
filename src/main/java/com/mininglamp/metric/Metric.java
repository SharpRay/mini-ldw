package com.mininglamp.metric;


import com.google.common.collect.Lists;

import java.util.List;

public abstract class Metric<T> {
    public enum MetricType {
        GAUGE, COUNTER
    }

    public enum MetricUnit {
        NANOSECONDS,
        MICROSECONDS,
        MILLISECONDS,
        SECONDS,
        BYTES,
        ROWS,
        PERCENT,
        REQUESTS,
        OPERATIONS,
        BLOCKS,
        ROWSETS,
        CONNECTIONS,
        PACKETS,
        NOUNIT
    };

    protected String name;
    protected MetricType type;
    protected MetricUnit unit;
    protected List<MetricLabel> labels = Lists.newArrayList();
    protected String description;

    public Metric(String name, MetricType type, MetricUnit unit, String description) {
        this.name = name;
        this.type = type;
        this.unit = unit;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public MetricType getType() {
        return type;
    }

    public MetricUnit getUnit() {
        return unit;
    }

    public String getDescription() {
        return description;
    }

    public Metric<T> addLabel(MetricLabel label) {
        if (labels.contains(label)) {
            return this;
        }
        labels.add(label);
        return this;
    }

    public List<MetricLabel> getLabels() {
        return labels;
    }

    public abstract T getValue();
}
