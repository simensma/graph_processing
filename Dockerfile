FROM neo4j:3.1.0

COPY target/graph-processing-2.3-SNAPSHOT.jar  /var/lib/neo4j/plugins/
