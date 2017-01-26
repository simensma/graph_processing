package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import com.maxdemarzi.processing.OpsRunner;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.maxdemarzi.processing.Utils.*;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorageParallelSPI implements PageRank {
    public static final int ONE_MINUS_ALPHA_INT = toInt(ONE_MINUS_ALPHA);
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private final ExecutorService pool;
    private final int relCount;
    private AtomicIntegerArray dst;

    public PageRankArrayStorageParallelSPI(GraphDatabaseService db, ExecutorService pool) {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount(db);
        this.relCount = new NodeCounter().getRelationshipCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {
        iterations = 200;

        int[] src = new int[nodeCount];
        dst = new AtomicIntegerArray(nodeCount);

        String[] labels = {"Profile", "Project"};
        String[] types = { "FOLLOWS", "COMMENTED_ON", "LICENSED" };


        try ( Transaction tx = db.beginTx()) {
            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            Integer[] labelIds = Arrays.stream(labels).map(ops::labelGetForName).toArray(Integer[]::new);
            Integer[] typeIds = Arrays.stream(types).map(ops::labelGetForName).toArray(Integer[]::new);

            int[] degrees = computeDegrees(ops,labelIds, typeIds);

            RelationshipVisitor<RuntimeException> visitor = (relId, relTypeId, startNode, endNode) -> {
                if (Arrays.stream(typeIds).anyMatch(t -> t == relTypeId)) {
                    dst.addAndGet(((int) endNode), src[(int) startNode]);
                }
            };

            for (int iteration = 0; iteration < iterations; iteration++) {
                startIteration(src, dst, degrees);

                PrimitiveLongIterator rels = ops.relationshipsGetAll();
                runOperations(pool, rels, relCount , ops, id -> ops.relationshipVisit(id, visitor));
            }
            tx.success();
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void startIteration(int[] src, AtomicIntegerArray dst, int[] degrees) {
        for (int node = 0; node < this.nodeCount; node++) {
            if (degrees[node] == -1) continue;
            src[node]= toInt(ALPHA * toFloat(dst.getAndSet(node, ONE_MINUS_ALPHA_INT)) / degrees[node]);

        }
    }

    private int[] computeDegrees(ReadOperations ops, Integer[] labelIds, Integer[] relationshipIds) throws EntityNotFoundException {
        int[] degree = new int[nodeCount];

        Arrays.fill(degree,-1);

        List<Integer[]> degrees = new ArrayList<>();

        for(Integer labelId: labelIds) {

            PrimitiveLongIterator it = ops.nodesGetForLabel(labelId);

            runOperations(pool, it, nodeCount, ops, id -> {
                for(Integer relationshipId: relationshipIds) {

                    int deg = ops.nodeGetDegree(id, Direction.OUTGOING, relationshipId);

                    if(deg <= 0) {
                        deg = ops.nodeGetDegree(id, Direction.INCOMING, relationshipId);
                    }

                    degrees.add(new Integer[] {id, deg});
                }
            });
        }

        degrees.forEach(e -> {
                if(degree[e[0]] < 0) {
                    degree[e[0]] = degree[e[1]];
                } else {
                    degree[e[0]] += degree[e[1]];
                }
            }
        );

        return degree;
    }

    @Override
    public double getResult(long node) {
        return dst != null ? toFloat(dst.get((int) node)) : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodeCount;
    }


}
