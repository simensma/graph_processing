package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import it.unimi.dsi.fastutil.doubles.Double2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import java.util.stream.StreamSupport;

public class PageRankMapStorage implements PageRank {
    private final GraphDatabaseService db;
    private final int nodes;
    private Long2DoubleMap dstMap;

    public PageRankMapStorage(GraphDatabaseService db) {
        this.db = db;
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {
        compute(new PageRankConfig(new String[]{label}, new String[]{type}, iterations));
    }

    public void compute(PageRankConfig config) {

        try ( Transaction tx = db.beginTx()) {
            List<Node> nodes = initializeNodes(config);
            dstMap = new Long2DoubleOpenHashMap(emptyNodeMap(nodes));
            Long2DoubleOpenHashMap srcMap = new Long2DoubleOpenHashMap(emptyNodeMap(nodes));

            List<RelationshipType> relTypes = initializeRelationshipTypes(config);
            Long2DoubleOpenHashMap weightMap = new Long2DoubleOpenHashMap(weightMap(config, nodes));

            for (int iteration = 0; iteration < config.getIterations(); iteration++) {
                perform_iteration(config, srcMap, weightMap, nodes, relTypes);
            }

            tx.success();
        }
    }

    private Map<Long, Double> emptyNodeMap(List<Node> nodes) {
        return nodes.stream().collect(Collectors.toMap(Node::getId, c -> 0.0));
    }

    private List<RelationshipType> initializeRelationshipTypes(PageRankConfig config) {
        return Arrays.stream(config.getRelationships())
                .map(RelationshipType::withName)
                .collect(Collectors.toList());
    }

    private Map<Long, Double> weightMap(PageRankConfig config, List<Node> nodes) {

        RelationshipType[] types = Arrays.stream(config.getRelationships())
                .map(RelationshipType::withName)
                .toArray(RelationshipType[]::new);

        return nodes.stream().collect(Collectors.toMap(Node::getId, n ->
                StreamSupport.stream(n.getRelationships(Direction.OUTGOING, types).spliterator(), false)
                        .mapToDouble((rel) ->
                                config.getWeights().get(rel.getType().name())
                        ).sum()
        ));
    }

    private List<Node> initializeNodes(PageRankConfig config) {
        return Arrays.stream(config.getLabels())
                .flatMap((l) -> db.findNodes(Label.label(l)).stream())
                .collect(Collectors.toList());
    }

    /**
     * PR(i)
     */
    private void perform_iteration(PageRankConfig config, Long2DoubleMap srcMap, Long2DoubleOpenHashMap weightMap, List<Node> nodes, List<RelationshipType> relTypes) {
        nodes.forEach(node -> {
            srcMap.put(node.getId(), ALPHA * dstMap.get(node.getId()));
            dstMap.put(node.getId(), ONE_MINUS_ALPHA);
        });

        db.getAllRelationships().stream()
            .filter(rel ->  relTypes.contains(rel.getType()))
            .forEach(rel -> {
                Node startNode = rel.getStartNode();
                Node endNode = rel.getEndNode();

                double totalStartNodeWeights = weightMap.get(startNode.getId());
                double relationshipWeight = config.getWeights().get(rel.getType().name());

                dstMap.put(startNode.getId(), (dstMap.get(endNode.getId()) + srcMap.get(startNode.getId()) * relationshipWeight / totalStartNodeWeights ));
            });
    }


    @Override
    public double getResult(long node) {
        return dstMap != null ? dstMap.getOrDefault(node, -1D) : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
