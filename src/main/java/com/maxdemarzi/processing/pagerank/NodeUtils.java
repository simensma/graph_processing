package com.maxdemarzi.processing.pagerank;

import org.neo4j.graphdb.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NodeUtils {
    private final GraphDatabaseService db;

    public NodeUtils(GraphDatabaseService db) {
        this.db = db;
    }

    public RelationshipType[] getRelationshipTypesWithLabels(String[] labels) {
        return Arrays.stream(labels)
                .map(RelationshipType::withName)
                .toArray(RelationshipType[]::new);
    }

    public List<Node> getNodesWithLabels(String[] labels) {
        return Arrays.stream(labels)
                .flatMap((l) -> db.findNodes(Label.label(l)).stream())
                .collect(Collectors.toList());
    }

    public Map<Long, Double> getEdgeWeightSumForNodes(List<Node> nodes, RelationshipType[] relTypes, Map<RelationshipType, Double> relWeights) {

        return nodes.stream().collect(Collectors.toMap(Node::getId, n ->
                getEdgeWeightSumForNode(n, relTypes, relWeights)
        ));
    }

    private double getEdgeWeightSumForNode(Node n, RelationshipType[] relTypes, Map<RelationshipType, Double> relWeights) {
        return StreamSupport.stream(n.getRelationships(Direction.OUTGOING, relTypes).spliterator(), false)
                .filter(r -> relWeights.containsKey(r.getType()))
                .mapToDouble(r -> relWeights.get(r.getType()))
                .sum();
    }

    public Map<RelationshipType, Double> getRelationshipTypeWeights(RelationshipType[] relTypes, Map<String, Double> weights, double defaultWeight) {
        return Arrays.stream(relTypes).collect(Collectors.toMap(Function.identity(), r -> {
            if(weights.containsKey(r.name())) {
                return weights.get(r.name());
            }

            return defaultWeight;
        }));
    }


}
