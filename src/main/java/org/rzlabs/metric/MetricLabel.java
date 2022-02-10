package org.rzlabs.metric;


// e.g. {job="load", type="hadoop", state="total"}
public class MetricLabel {
    private String key;
    private String value;

    public MetricLabel(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MetricLabel)) {
            return false;
        }

        MetricLabel other = (MetricLabel) obj;
        if (this.key.equalsIgnoreCase(other.key)) {
            return true;
        }
        return false;
    }
}
