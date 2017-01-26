package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorageSPI implements PageRank {
    private final GraphDatabaseAPI db;
    private final int nodes;
    private float[] dst;

    public PageRankArrayStorageSPI(GraphDatabaseService db) {
        this.db = (GraphDatabaseAPI) db;
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {

        float[] src = new float[nodes];
        dst = new float[nodes];

        String[] labels = {"Profile", "Project"};
        String[] types = { "FOLLOWS", "COMMENTED_ON", "LICENSED" };

        try ( Transaction tx = db.beginTx()) {

            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();

            Integer[] labelIds = Arrays.stream(labels).map(ops::labelGetForName).toArray(Integer[]::new);
            Integer[] typeIds = Arrays.stream(types).map(ops::labelGetForName).toArray(Integer[]::new);

            int[] degrees = computeDegrees(ops,labelIds, typeIds);

            RelationshipVisitor<RuntimeException> visitor = new RelationshipVisitor<RuntimeException>() {
                public void visit(long relId, int relTypeId, long startNode, long endNode) throws RuntimeException {
                    if (Arrays.stream(typeIds).anyMatch(t -> t == relTypeId)) {
                        dst[((int) endNode)] += src[(int) startNode];
                    }
                }
            };

            for (int iteration = 0; iteration < iterations; iteration++) {
                startIteration(src, dst, degrees);

                PrimitiveLongIterator rels = ops.relationshipsGetAll();
                while (rels.hasNext()) {
                    ops.relationshipVisit(rels.next(), visitor);
                }
            }
            tx.success();
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void startIteration(float[] src, float[] dst, int[] degrees) {
        for (int node = 0; node < this.nodes; node++) {
            if (degrees[node] == -1) continue;
            src[node]= (float) (ALPHA * dst[node] / degrees[node]);
        }
        Arrays.fill(dst, (float) ONE_MINUS_ALPHA);
    }

    private int[] computeDegrees(ReadOperations ops, Integer[] labelIds, Integer[] relationshipIds) throws EntityNotFoundException {
        int[] degrees = new int[nodes];
        Arrays.fill(degrees,-1);

        for(int labelId: labelIds){

            for(int relationshipId: relationshipIds) {
                PrimitiveLongIterator nodes = ops.nodesGetForLabel(labelId);
                while (nodes.hasNext()) {
                    long node = nodes.next();

                    int outDegree = ops.nodeGetDegree(node, Direction.OUTGOING, relationshipId);

                    if(outDegree <= 0){
                        outDegree = ops.nodeGetDegree(node, Direction.INCOMING, relationshipId);
                    }

                    if(degrees[((int)node)] < 0) {
                        degrees[((int)node)] = outDegree;
                    } else {
                        degrees[((int) node)] += outDegree;
                    }
                }
            }

        }
        return degrees;
    }

    @Override
    public double getResult(long node) {
        return dst != null ? dst[((int) node)] : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
