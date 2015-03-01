package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test of the MiniSolrCloudCluster functionality. Keep in mind, 
 * MiniSolrCloudCluster is designed to be used outside of the Lucene test
 * hierarchy.
 */
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestMiniSolrCloudCluster extends LuceneTestCase {

  private static Logger log = LoggerFactory.getLogger(MiniSolrCloudCluster.class);
  private static final int NUM_SERVERS = 5;
  private static final int NUM_SHARDS = 2;
  private static final int REPLICATION_FACTOR = 2;
  private static MiniSolrCloudCluster miniCluster;

  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());
  
  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @BeforeClass
  public static void startup() throws Exception {
    File solrXml = new File(SolrTestCaseJ4.TEST_HOME(), "solr-no-core.xml");
    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, createTempDir().toFile(), solrXml, null, null);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
  }

  @Test
  public void testBasics() throws Exception {
    assertNotNull(miniCluster.getZkServer());
    List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
    assertEquals(NUM_SERVERS, jettys.size());
    for (JettySolrRunner jetty : jettys) {
      assertTrue(jetty.isRunning());
    }

    // shut down a server
    JettySolrRunner stoppedServer = miniCluster.stopJettySolrRunner(0);
    assertTrue(stoppedServer.isStopped());
    assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());

    // create a server
    JettySolrRunner startedServer = miniCluster.startJettySolrRunner(null, null, null);
    assertTrue(startedServer.isRunning());
    assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

    // create collection
    String collectionName = "testSolrCloudCollection";
    String configName = "solrCloudCollectionConfig";
    File configDir = new File(SolrTestCaseJ4.TEST_HOME() + File.separator + "collection1" + File.separator + "conf");
    miniCluster.uploadConfigDir(configDir, configName);
    
    Map<String, String> collectionProperties = new HashMap<>();
    collectionProperties.put(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
    collectionProperties.put("solr.tests.maxBufferedDocs", "100000");
    collectionProperties.put("solr.tests.maxIndexingThreads", "-1");
    collectionProperties.put("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    collectionProperties.put("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
    collectionProperties.put("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    collectionProperties.put("solr.directoryFactory", "solr.RAMDirectoryFactory");
    miniCluster.createCollection(collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName, collectionProperties);
    
    try(SolrZkClient zkClient = new SolrZkClient
        (miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, 45000, null)) {
      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);
      
      // modify/query collection
      CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();
      cloudSolrClient.setDefaultCollection(collectionName);
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "1");
      cloudSolrClient.add(doc);
      cloudSolrClient.commit();
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");
      QueryResponse rsp = cloudSolrClient.query(query);
      assertEquals(1, rsp.getResults().getNumFound());

      // remove a server not hosting any replicas
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      HashMap<String, JettySolrRunner> jettyMap = new HashMap<String, JettySolrRunner>();
      for (JettySolrRunner jetty : miniCluster.getJettySolrRunners()) {
        String key = jetty.getBaseUrl().toString().substring((jetty.getBaseUrl().getProtocol() + "://").length());
        jettyMap.put(key, jetty);
      }
      Collection<Slice> slices = clusterState.getSlices(collectionName);
      // track the servers not host repliacs
      for (Slice slice : slices) {
        jettyMap.remove(slice.getLeader().getNodeName().replace("_solr", "/solr"));
        for (Replica replica : slice.getReplicas()) {
          jettyMap.remove(replica.getNodeName().replace("_solr", "/solr"));
        }
      }
      assertTrue("Expected to find a node without a replica", jettyMap.size() > 0);
      JettySolrRunner jettyToStop = jettyMap.entrySet().iterator().next().getValue();
      jettys = miniCluster.getJettySolrRunners();
      for (int i = 0; i < jettys.size(); ++i) {
        if (jettys.get(i).equals(jettyToStop)) {
          miniCluster.stopJettySolrRunner(i);
          assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());
        }
      }
    }
  }

}
