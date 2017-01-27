Graph Processing
================

This is an unmanaged extension with Graph Processing Algorithms on top of Neo4j.

- [![Build Status](https://secure.travis-ci.org/maxdemarzi/graph_processing.png?branch=master)](http://travis-ci.org/maxdemarzi/graph_processing)

# Quick Start

## WITH DOCKER

1. Build it:

mvn clean package docker:build

2. Start Neo4j Server:

        docker run \
        --publish=7474:7474 --publish=7687:7687 \
        --volume=$HOME/neo4j/data:/data \
        --volume=$(pwd)/conf:/conf \
        --volume=$HOME/neo4j/logs:/logs \
        graphprocessing

3. Create the Movie Dataset:

        :play movies

4. Create KNOWS relationships amongst actors:

        MATCH (a1:Person)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)
        CREATE (a1)-[:KNOWS]->(coActors);

5. Call the pagerank endpoint:

        curl http://neo4j:neo4j@localhost:7474/service/v1/pagerank/Person/KNOWS on Linux

        or

        curl http://$(docker-machine ip default):7474/service/v1/pagerank/Person/KNOWS on OSX


You should see "PageRank for Person and KNOWS Completed!"

6. Check the pageranks of some nodes:

        MATCH (n:Person) RETURN n ORDER BY n.pagerank DESC LIMIT 10;



## Without Docker

Hmpf.

1. Build it:

        mvn clean package

2. Copy target/graph-processing-1.0.jar to the plugins/ directory of your Neo4j server.

        mv target/graph-processing-1.0.jar neo4j/plugins/.

3. Have some coffee

3.2. Install Neo4j

4. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi.processing=/service


5. Start Neo4j server.


6. Create the Movie Dataset:

        :play movies

7. Create KNOWS relationships amongst actors:
        MATCH (a1:Person)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)
        CREATE (a1)-[:KNOWS]->(coActors);

8. Call the pagerank endpoint:
        curl http://neo4j:neo4j@localhost:7474/service/v1/pagerank/Person/KNOWS on Linux

        or

        curl http://$(docker-machine ip default):7474/service/v1/pagerank/Person/KNOWS on OSX

You should see "PageRank for Person and KNOWS Completed!"

9. Check the pageranks of some nodes:

        MATCH (n:Person) RETURN n ORDER BY n.pagerank DESC LIMIT 10;


# Algorithms Implemented

- Page Rank
- Weighted PageRank
- Label Propagation
- Union Find
- Betweenness Centrality
- Closeness Centrality
- Degree Centrality



# Endpoints

Replace "swordfish" below with your neo4j password.  The available endpoints are:


        curl http://neo4j:swordfish@localhost:7474/service/v1/pagerank/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/labelpropagation/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/unionfind/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/centrality/betweenness/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/centrality/closeness/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/centrality/degree/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/centrality/indegree/{Label}/{RelationshipType}
        curl http://neo4j:swordfish@localhost:7474/service/v1/centrality/outdegree/{Label}/{RelationshipType}

An optional query parameter "iterations" has a default of 20.

        curl http://neo4j:swordfish@localhost:7474/service/v1/pagerank/{Label}/{RelationshipType}?iterations=25

** Weighted Pagerank **

```curl
curl -X POST -H "Content-Type: application/json" -d '{
  "labels": ["Person"],
  "relationships": ["KNOWS"],
  "iterations": 200,
  "weights": {
    "KNOWS": 1.0,
  }
}' "http://neo4j:neo4j@localhost:7474/service/v1/weightedpagerank/"
```

# Performance

There are some JMH performance tests included in this repository.
Use [IntelliJ](https://www.jetbrains.com/idea/ "IntelliJ") and run them with the [JMH Plugin](https://github.com/artyushov/idea-jmh-plugin "JMH Plugin").

# Todo
* https://graph-tool.skewed.de/static/doc/index.html
http://snap.stanford.edu/proj/snap-icwsm/SNAP-ICWSM14.pdf
* Convert Centrality to use Cursor SPI
* Other Graph Algorithms
