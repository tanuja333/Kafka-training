package com.cloudera.training.kafka;

import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.log4j.Logger;

public class MetricsLogger {
    private TreeSet<String> desiredMetrics = new TreeSet<>();
    private Logger metricLogger;
    private int delayMillis = 0;
    Date prevMetricsDate;
    // metricsBatchName can be used to differentiate sets of metrics later when analyzing
    private String metricsBatchName = "";

    public MetricsLogger(String loggerName, int delayMillis) {
        metricLogger = Logger.getLogger(loggerName);
        this.delayMillis = delayMillis;
    }

    public MetricsLogger(String loggerName) {
        this(loggerName, 0);
    }


    public void setMetrics(List<String> metricNames) {
        // Example: setMetrics(Arrays.asList("batch-size-avg"));
        desiredMetrics = new TreeSet<>(metricNames);
    }

    public void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        Date rightNow = new Date();
        if (this.prevMetricsDate == null || rightNow.getTime() - this.prevMetricsDate.getTime() > delayMillis) {
            for (MetricName mn : metrics.keySet()) {
                Metric m = metrics.get(mn);
                if (desiredMetrics.isEmpty() || desiredMetrics.contains(m.metricName().name())) {
                    String metricOutput = String.format(Locale.US, "%s\t%.2f\t%s\t%s",
                            mn.name() + mn.tags().getOrDefault("node-id", ""), 
                            m.value(), 
                            mn.description(),
                            this.metricsBatchName);
                    metricLogger.info(metricOutput);
                }
            }
            this.prevMetricsDate = new Date();
        }
    }

    public String getMetricsBatchName() {
        return metricsBatchName;
    }

    public void setMetricsBatchName(String metricsBatchName) {
        this.metricsBatchName = metricsBatchName;
    }
}
