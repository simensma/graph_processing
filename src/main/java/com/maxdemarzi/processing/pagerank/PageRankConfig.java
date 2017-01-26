package com.maxdemarzi.processing.pagerank;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;
import java.util.Map;

@XmlRootElement
public class PageRankConfig {

    @XmlElement
    private String[] labels;

    @XmlElement
    private String[] relationships;

    @XmlElement
    private Map<String, Double> weights;

    @XmlElement
    private int iterations;

    public PageRankConfig(String[] labels, String[] relationships, int iterations) {
        this.labels = labels;
        this.relationships = relationships;
        this.iterations = iterations;
    }

    public PageRankConfig() {}

    public String[] getLabels() {
        return labels;
    }

    public String[] getRelationships() {
        return relationships;
    }

    public int getIterations() {
        return iterations;
    }

    public void setLabels(String[] labels) {
        this.labels = labels;
    }

    public void setRelationships(String[] relationships) {
        this.relationships = relationships;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    @Override
    public String toString() {
        return "PageRankConfig{" +
                "labels=" + Arrays.toString(labels) +
                ", relationships=" + Arrays.toString(relationships) +
                ", weights=" + weights +
                ", iterations=" + iterations +
                '}';
    }

    public Map<String, Double> getWeights() {
        return weights;
    }

    public void setWeights(Map<String, Double> weights) {
        this.weights = weights;
    }
}
