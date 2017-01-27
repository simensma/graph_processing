package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import it.unimi.dsi.fastutil.longs.*;
import org.neo4j.graphdb.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * - Weighted PageRank
 *
 * Instead of distributing the PageRank score of a node evenly to all its "outgoing neighbours",
 * this approach distributes the node's PageRank based on the normalized weight of the out-edge between them instead
 *
 * The original PageRank algorithm as outlined by Page and Brin:
 *
 * PR(Ni) = (1-α) + α*Σ(Nj ϵ M(Ni)) PR(Nj)/L(Nj)
 *
 * N1, N2,..., Nn are the nodes we want to calculate a score for
 * PR(Ni) is the pagerank for Node Ni
 * α is the damping factor
 * N is the total number of nodes we want to calculate a score for
 * M(Ni) is the set of nodes that have an outbound edge to node i
 * L(Nj) is the number of outbound edge to node Nj
 *
 * To accommodate for weighted edges, we modify the original PageRank algorithm as follows:
 *
 * PR(Ni) = (1-α) + α*Σ(Nj ϵ M(Ni)) PR(Nj) * LW(Nj, Ni)/SUM(LW(Nj))
 *
 * LW(Nj, Ni) is the weight of the edge going from node Nj to Ni
 * SUM(LW(Nj)) is the sum of the weights of all the outgoing relationships from Nj
 *
 * REF: The Anatomy of a Large-Scale Hypertextual Web Search Engine - Brin, Page, 1999
 */
public class PageRankMapStorage implements PageRank {
    private final GraphDatabaseService db;
    private final int nodes;
    private final NodeUtils nodeUtils;
    private Long2DoubleMap pageranks = new Long2DoubleOpenHashMap();
    private static final double DEFAULT_EDGE_WEIGHT = 1.0;

    public PageRankMapStorage(GraphDatabaseService db) {
        this.db = db;
        this.nodeUtils = new NodeUtils(db);
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {
        compute(new PageRankConfig(new String[]{label}, new String[]{type}, iterations));
    }


    /**
     * Iteratively calculates the weighted PageRank of the nodes with labels config.getLabels()
     * based on the relationships specified by config.getRelationships(), and the relationship weights
     * specified by config.getWeights()
     *
     * @param config The configuration to calculate PageRanks on
     *      config.getLabels() -> The labels of the nodes we want to include in the calculation
     *               Example: ["Profile", "Project"]
     *      config.getRelationships() -> The relationship types that should be included in the PageRank calculation
     *               Example: ["COMMENTED_ON", "FOLLOWS", "LIKES"]
     *      config.getIterations() -> The number of iterations we should run the algorithm for.
     *      config.getWeights() -> The weights of each relationship type.
     *               Default value for a relationship type is 1.0
     *               Example: {
     *                  "COMMENTED_ON": 0.5,
     *                  "FOLLOWS": 5.0,
     *                  "LIKES": 3.0
     *               }
     */
    public void compute(PageRankConfig config) {

        try ( Transaction tx = db.beginTx()) {
            List<Node> nodes = nodeUtils.getNodesWithLabels(config.getLabels());

            RelationshipType[] relTypes = nodeUtils.getRelationshipTypesWithLabels(config.getRelationships());
            List<RelationshipType> relTypesList = Arrays.asList(relTypes);

            Map<RelationshipType, Double> relWeights = nodeUtils.getRelationshipTypeWeights(relTypes, config.getWeights(), DEFAULT_EDGE_WEIGHT);

            Long2DoubleOpenHashMap totalWeights = new Long2DoubleOpenHashMap(nodeUtils.getEdgeWeightSumForNodes(nodes, relTypes, relWeights));

            for (int iteration = 0; iteration < config.getIterations(); iteration++) {
                performIteration(totalWeights, nodes, relTypesList, relWeights);
            }

            tx.success();
        }
    }

    /**
     * Performs an iteration (t) of the weighted PageRank algorithm
     *
     *  PR(Ni; 0) = 1 - α
     *  PR(Ni; t + 1) = (1-α) + α*Σ(Nj ϵ M(Ni)) PR(Nj; t) * LW(Nj, Ni)/SUM(LW(Nj))
     *
     * @param totalWeights Map: (NodeId -> sum of the weight of the outgoing relationships for that node)
     * @param nodes List of all Nodes to be considered
     * @param relTypes List off all relationships to be considered
     * @param relWeights Relationship weights for all relationships to be considered
     */
    private void performIteration(Long2DoubleOpenHashMap totalWeights, List<Node> nodes, List<RelationshipType> relTypes, Map<RelationshipType, Double> relWeights) {
        Long2DoubleOpenHashMap srcMap = new Long2DoubleOpenHashMap();

        // Precalculate α * PR(Nj;t)
        nodes.forEach(node -> {
            srcMap.put(node.getId(), ALPHA * pageranks.get(node.getId()));
            pageranks.put(node.getId(), ONE_MINUS_ALPHA);
        });

        // Transfer scores between nodes that are connected
        db.getAllRelationships().stream()
            .filter(rel ->  relTypes.contains(rel.getType()))
            .forEach(rel -> transferWeightsForRelationship(rel, totalWeights, srcMap, relWeights));
    }

    private void transferWeightsForRelationship(Relationship rel, Long2DoubleOpenHashMap totalWeights, Long2DoubleMap srcMap, Map<RelationshipType, Double> relWeights) {
        Node startNode = rel.getStartNode();
        Node endNode = rel.getEndNode();

        double relationshipWeight = relWeights.get(rel.getType());

        double totalStartNodeWeights = totalWeights.get(startNode.getId());
        double currentStartNodeWeight = srcMap.get(startNode.getId());
        double currentEndNodeWeight = pageranks.get(endNode.getId());

        // Transfer α * PR(startNode) * LW(rel)/SUM(LW(startNode)) from startNode to endNode
        pageranks.put(endNode.getId(), currentEndNodeWeight + currentStartNodeWeight*relationshipWeight / totalStartNodeWeights);

    }


    @Override
    public double getResult(long node) {
        return pageranks != null ? pageranks.getOrDefault(node, -1D) : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
