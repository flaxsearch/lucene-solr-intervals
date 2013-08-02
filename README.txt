Apache Lucene/Solr - positions fork

lucene/ is a search engine library
solr/ is a search engine server that uses lucene

This version of lucene/solr is a fork based on
https://issues.apache.org/jira/browse/LUCENE-2878, that allows consumer code to
iterate through individual hit positions on a searcher match.

To build:

cd maven-build
mvn -DskipTests install
