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

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

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
    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, createTempDir(), solrXml, null, null);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    System.clearProperty("solr.tests.mergePolicy");
    System.clearProperty("solr.tests.maxBufferedDocs");
    System.clearProperty("solr.tests.maxIndexingThreads");
    System.clearProperty("solr.tests.ramBufferSizeMB");
    System.clearProperty("solr.tests.mergeScheduler");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.solrxml.location");
    System.clearProperty("zkHost");
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

    CloudSolrServer cloudSolrServer = null;
    SolrZkClient zkClient = null;
    try {
      cloudSolrServer = new CloudSolrServer(miniCluster.getZkServer().getZkAddress(), true);
      cloudSolrServer.connect();
      zkClient = new SolrZkClient(miniCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, 45000, null);

      // create collection
      String collectionName = "testSolrCloudCollection";
      String configName = "solrCloudCollectionConfig";
      System.setProperty("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
      uploadConfigToZk(SolrTestCaseJ4.TEST_HOME() + File.separator + "collection1" + File.separator + "conf", configName);
      createCollection(cloudSolrServer, collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName);

      // modify/query collection
      cloudSolrServer.setDefaultCollection(collectionName);
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "1");

      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);
      cloudSolrServer.add(doc);
      cloudSolrServer.commit();
      SolrQuery query = new SolrQuery();
      query.setQuery("*:*");
      QueryResponse rsp = cloudSolrServer.query(query);
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
    } finally {
      if (cloudSolrServer != null) {
        cloudSolrServer.shutdown();
      }
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  protected void uploadConfigToZk(String configDir, String configName) throws Exception {
    // override settings in the solrconfig include
    System.setProperty("solr.tests.maxBufferedDocs", "100000");
    System.setProperty("solr.tests.maxIndexingThreads", "-1");
    System.setProperty("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");

    SolrZkClient zkClient = null;
    try {
      zkClient =  new SolrZkClient(miniCluster.getZkServer().getZkAddress(), AbstractZkTestCase.TIMEOUT, 45000, null);
      uploadConfigFileToZk(zkClient, configName, "solrconfig.xml", new File(configDir, "solrconfig-tlog.xml"));
      uploadConfigFileToZk(zkClient, configName, "schema.xml", new File(configDir, "schema.xml"));
      uploadConfigFileToZk(zkClient, configName, "solrconfig.snippet.randomindexconfig.xml",
        new File(configDir, "solrconfig.snippet.randomindexconfig.xml"));
      uploadConfigFileToZk(zkClient, configName, "currency.xml", new File(configDir, "currency.xml"));
      uploadConfigFileToZk(zkClient, configName, "mapping-ISOLatin1Accent.txt",
        new File(configDir, "mapping-ISOLatin1Accent.txt"));
      uploadConfigFileToZk(zkClient, configName, "old_synonyms.txt", new File(configDir, "old_synonyms.txt"));
      uploadConfigFileToZk(zkClient, configName, "open-exchange-rates.json",
        new File(configDir, "open-exchange-rates.json"));
      uploadConfigFileToZk(zkClient, configName, "protwords.txt", new File(configDir, "protwords.txt"));
      uploadConfigFileToZk(zkClient, configName, "stopwords.txt", new File(configDir, "stopwords.txt"));
      uploadConfigFileToZk(zkClient, configName, "synonyms.txt", new File(configDir, "synonyms.txt"));
    } finally {
      if (zkClient != null) zkClient.close();
    }
  }

  protected void uploadConfigFileToZk(SolrZkClient zkClient, String configName, String nameInZk, File file)
      throws Exception {
    zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + configName + "/" + nameInZk, file, false, true);
  }

  protected NamedList<Object> createCollection(CloudSolrServer server, String name, int numShards,
      int replicationFactor, String configName) throws Exception {
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
    modParams.set("name", name);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);
    modParams.set("collection.configName", configName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    return server.request(request);
  }

  protected void waitForRecoveriesToFinish(String collection,
      ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout, int timeoutSeconds)
      throws Exception {
    log.info("Wait for recoveries to finish - collection: " + collection + " failOnTimeout:" + failOnTimeout + " timeout (sec):" + timeoutSeconds);
    boolean cont = true;
    int cnt = 0;
    
    while (cont) {
      if (verbose) System.out.println("-");
      boolean sawLiveRecovering = false;
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      assertNotNull("Could not find collection:" + collection, slices);
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,Replica> shards = entry.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : shards.entrySet()) {
          if (verbose) System.out.println("rstate:"
              + shard.getValue().getStr(ZkStateReader.STATE_PROP)
              + " live:"
              + clusterState.liveNodesContain(shard.getValue().getNodeName()));
          String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
          if ((state.equals(ZkStateReader.RECOVERING) || state
              .equals(ZkStateReader.SYNC) || state.equals(ZkStateReader.DOWN))
              && clusterState.liveNodesContain(shard.getValue().getStr(
              ZkStateReader.NODE_NAME_PROP))) {
            sawLiveRecovering = true;
          }
        }
      }
      if (!sawLiveRecovering || cnt == timeoutSeconds) {
        if (!sawLiveRecovering) {
          if (verbose) System.out.println("no one is recoverying");
        } else {
          if (verbose) System.out.println("Gave up waiting for recovery to finish..");
          if (failOnTimeout) {
            fail("There are still nodes recoverying - waited for " + timeoutSeconds + " seconds");
            // won't get here
            return;
          }
        }
        cont = false;
      } else {
        Thread.sleep(1000);
      }
      cnt++;
    }

    log.info("Recoveries finished - collection: " + collection);
  }
}
