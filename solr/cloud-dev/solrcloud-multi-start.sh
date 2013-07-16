#!/usr/bin/env bash

# starts up the multicore example

cd ..

rm -r -f example2
rm -r -f example3
rm -r -f example4
rm -r -f example5
rm -r -f example6

rm -r -f dist
rm -r -f build
rm -r -f example/solr/zoo_data
rm -r -f example/solr/collection1/data
rm -f example/example.log

ant example dist

cp -r -f example example2
cp -r -f example example3
cp -r -f example example4
cp -r -f example example5
cp -r -f example example6

java -classpath "example/solr-webapp/webapp/WEB-INF/lib/*:example/lib/ext/" org.apache.solr.cloud.ZkCLI -cmd upconf -zkhost 127.0.0.1:9983 -solrhome example/multicore -runzk 8983

cd example
java -DzkRun -DnumShards=2 -DSTOP.PORT=7983 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example.log 2>&1 &

cd ../example2
java -Djetty.port=7574 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6574 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example2.log 2>&1 &

cd ../example3
java -Djetty.port=7575 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6575 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example3.log 2>&1 &

cd ../example4
java -Djetty.port=7576 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6576 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example4.log 2>&1 &

cd ../example5
java -Djetty.port=7577 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6577 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example5.log 2>&1 &

cd ../example6
java -Djetty.port=7578 -DzkHost=localhost:9983 -DnumShards=2 -DSTOP.PORT=6578 -DSTOP.KEY=key -Dsolr.solr.home=multicore -jar start.jar 1>example6.log 2>&1 &
