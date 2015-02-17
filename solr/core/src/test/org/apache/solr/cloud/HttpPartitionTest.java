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

import org.apache.http.NoHttpResponseException;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Simulates HTTP partitions between a leader and replica but the replica does
 * not lose its ZooKeeper connection.
 */

@Slow
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class HttpPartitionTest extends AbstractFullDistribZkTestBase {
  
  protected static final transient Logger log =
      LoggerFactory.getLogger(HttpPartitionTest.class);
  
  // To prevent the test assertions firing too fast before cluster state
  // recognizes (and propagates) partitions
  protected static final long sleepMsBeforeHealPartition = 2000L;

  protected static final int maxWaitSecsToSeeAllActive = 30;

  public HttpPartitionTest() {
    super();
    sliceCount = 2;
    fixShardCount(3);
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  /**
   * Overrides the parent implementation to install a SocketProxy in-front of the Jetty server.
   */
  @Override
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception
  {
    return createProxiedJetty(solrHome, dataDir, shardList, solrConfigOverride, schemaOverride);
  }

  @Test
  public void test() throws Exception {
    waitForThingsToLevelOut(30000);

    testLeaderInitiatedRecoveryCRUD();

    // test a 1x2 collection
    testRf2();

    waitForThingsToLevelOut(30000);

    // now do similar for a 1x3 collection while taking 2 replicas on-and-off
    // each time
    testRf3();

    waitForThingsToLevelOut(30000);

    // have the leader lose its Zk session temporarily
    testLeaderZkSessionLoss();

    waitForThingsToLevelOut(30000);

    log.info("HttpParitionTest succeeded ... shutting down now!");
  }

  /**
   * Tests handling of lir state znodes.
   */
  protected void testLeaderInitiatedRecoveryCRUD() throws Exception {
    String testCollectionName = "c8n_crud_1x2";
    String shardId = "shard1";
    createCollectionRetry(testCollectionName, 1, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    Replica leader =
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));

    CoreContainer cores = ((SolrDispatchFilter)leaderJetty.getDispatchFilter().getFilter()).getCores();
    ZkController zkController = cores.getZkController();
    assertNotNull("ZkController is null", zkController);

    Replica notLeader =
        ensureAllReplicasAreActive(testCollectionName, shardId, 1, 2, maxWaitSecsToSeeAllActive).get(0);

    ZkCoreNodeProps replicaCoreNodeProps = new ZkCoreNodeProps(notLeader);
    String replicaUrl = replicaCoreNodeProps.getCoreUrl();

    assertTrue(!zkController.isReplicaInRecoveryHandling(replicaUrl));
    assertTrue(zkController.ensureReplicaInLeaderInitiatedRecovery(testCollectionName, shardId, replicaUrl, replicaCoreNodeProps, false));
    assertTrue(zkController.isReplicaInRecoveryHandling(replicaUrl));
    Map<String,Object> lirStateMap = zkController.getLeaderInitiatedRecoveryStateObject(testCollectionName, shardId, notLeader.getName());
    assertNotNull(lirStateMap);
    assertEquals(ZkStateReader.DOWN, lirStateMap.get("state"));
    zkController.removeReplicaFromLeaderInitiatedRecoveryHandling(replicaUrl);
    assertTrue(!zkController.isReplicaInRecoveryHandling(replicaUrl));

    // test old non-json format handling
    SolrZkClient zkClient = zkController.getZkClient();
    String znodePath = zkController.getLeaderInitiatedRecoveryZnodePath(testCollectionName, shardId, notLeader.getName());
    zkClient.setData(znodePath, "down".getBytes(StandardCharsets.UTF_8), true);
    lirStateMap = zkController.getLeaderInitiatedRecoveryStateObject(testCollectionName, shardId, notLeader.getName());
    assertNotNull(lirStateMap);
    assertEquals(ZkStateReader.DOWN, lirStateMap.get("state"));
    zkClient.delete(znodePath, -1, false);

    // try to clean up
    try {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  protected void testRf2() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x2";
    createCollectionRetry(testCollectionName, 1, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);
    
    Replica notLeader = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive).get(0);
    
    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy = getProxyForReplica(notLeader);
    
    proxy.close();
    
    // indexing during a partition
    sendDoc(2);
    
    // Have the partition last at least 1 sec
    // While this gives the impression that recovery is timing related, this is
    // really only
    // to give time for the state to be written to ZK before the test completes.
    // In other words,
    // without a brief pause, the test finishes so quickly that it doesn't give
    // time for the recovery process to kick-in
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy.reopen();
    
    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    
    sendDoc(3);
    
    // sent 3 docs in so far, verify they are on the leader and replica
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 3);
        
    // now up the stakes and do more docs
    int numDocs = 1000;
    boolean hasPartition = false;
    for (int d = 0; d < numDocs; d++) {
      // create / restore partition every 100 docs
      if (d % 100 == 0) {
        if (hasPartition) {
          proxy.reopen();
          hasPartition = false;
        } else {
          if (d >= 100) {
            proxy.close();
            hasPartition = true;
            Thread.sleep(sleepMsBeforeHealPartition);
          }
        }
      }
      sendDoc(d + 4); // 4 is offset as we've already indexed 1-3
    }
    
    // restore connectivity if lost
    if (hasPartition) {
      proxy.reopen();
    }
    
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    
    // verify all docs received
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, numDocs + 3);

    log.info("testRf2 succeeded ... deleting the "+testCollectionName+" collection");

    // try to clean up
    try {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }
  
  protected void testRf3() throws Exception {
    // create a collection that has 1 shard but 2 replicas
    String testCollectionName = "c8n_1x3";
    createCollectionRetry(testCollectionName, 1, 3, 1);
    
    cloudClient.setDefaultCollection(testCollectionName);
    
    sendDoc(1);

    List<Replica> notLeaders = 
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive);
    assertTrue("Expected 2 replicas for collection " + testCollectionName
        + " but found " + notLeaders.size() + "; clusterState: "
        + printClusterStateInfo(testCollectionName),
        notLeaders.size() == 2);

    // ok, now introduce a network partition between the leader and the replica
    SocketProxy proxy0 = getProxyForReplica(notLeaders.get(0));
    
    proxy0.close();
    
    // indexing during a partition
    sendDoc(2);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    
    proxy0.reopen();
    
    SocketProxy proxy1 = getProxyForReplica(notLeaders.get(1));
    
    proxy1.close();
    
    sendDoc(3);
    
    Thread.sleep(sleepMsBeforeHealPartition);
    proxy1.reopen();
    
    // sent 4 docs in so far, verify they are on the leader and replica
    notLeaders = ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 3, maxWaitSecsToSeeAllActive); 
    
    sendDoc(4);
    
    assertDocsExistInAllReplicas(notLeaders, testCollectionName, 1, 4);

    log.info("testRf3 succeeded ... deleting the "+testCollectionName+" collection");

    // try to clean up
    try {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  private void createCollectionRetry(String testCollectionName, int numShards, int replicationFactor, int maxShardsPerNode)
      throws SolrServerException, IOException {
    CollectionAdminResponse resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);
    if (resp.getResponse().get("failure") != null) {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
      
      resp = createCollection(testCollectionName, numShards, replicationFactor, maxShardsPerNode);
      
      if (resp.getResponse().get("failure") != null) {
        fail("Could not create " + testCollectionName);
      }
    }
  }

  // test inspired by SOLR-6511
  protected void testLeaderZkSessionLoss() throws Exception {

    String testCollectionName = "c8n_1x2_leader_session_loss";
    createCollectionRetry(testCollectionName, 1, 2, 1);
    cloudClient.setDefaultCollection(testCollectionName);

    sendDoc(1);

    List<Replica> notLeaders =
        ensureAllReplicasAreActive(testCollectionName, "shard1", 1, 2, maxWaitSecsToSeeAllActive);
    assertTrue("Expected 1 replicas for collection " + testCollectionName
            + " but found " + notLeaders.size() + "; clusterState: "
            + printClusterStateInfo(testCollectionName),
        notLeaders.size() == 1);

    Replica leader =
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    String leaderNode = leader.getNodeName();
    assertNotNull("Could not find leader for shard1 of "+
        testCollectionName+"; clusterState: "+printClusterStateInfo(testCollectionName), leader);
    JettySolrRunner leaderJetty = getJettyOnPort(getReplicaPort(leader));


    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(2));
    doc.addField("a_t", "hello" + 2);

    // cause leader migration by expiring the current leader's zk session
    chaosMonkey.expireSession(leaderJetty);

    String expectedNewLeaderCoreNodeName = notLeaders.get(0).getName();
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(60, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      String currentLeaderName = null;
      try {
        Replica currentLeader =
            cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
        currentLeaderName = currentLeader.getName();
      } catch (Exception exc) {}

      if (expectedNewLeaderCoreNodeName.equals(currentLeaderName))
        break; // new leader was elected after zk session expiration

      Thread.sleep(500);
    }

    Replica currentLeader =
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1");
    assertEquals(expectedNewLeaderCoreNodeName, currentLeader.getName());

    // TODO: This test logic seems to be timing dependent and fails on Jenkins
    // need to come up with a better approach
    log.info("Sending doc 2 to old leader "+leader.getName());
    try ( HttpSolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName)) {
    
      leaderSolr.add(doc);
      leaderSolr.close();

      // if the add worked, then the doc must exist on the new leader
      try (HttpSolrClient newLeaderSolr = getHttpSolrClient(currentLeader, testCollectionName)) {
        assertDocExists(newLeaderSolr, testCollectionName, "2");
      }

    } catch (SolrException exc) {
      // this is ok provided the doc doesn't exist on the current leader
      try (HttpSolrClient client = getHttpSolrClient(currentLeader, testCollectionName)) {
        client.add(doc); // this should work
      }
    } 

    List<Replica> participatingReplicas = getActiveOrRecoveringReplicas(testCollectionName, "shard1");
    Set<String> replicasToCheck = new HashSet<>();
    for (Replica stillUp : participatingReplicas)
      replicasToCheck.add(stillUp.getName());
    waitToSeeReplicasActive(testCollectionName, "shard1", replicasToCheck, 20);
    assertDocsExistInAllReplicas(participatingReplicas, testCollectionName, 1, 2);

    log.info("testLeaderZkSessionLoss succeeded ... deleting the "+testCollectionName+" collection");

    // try to clean up
    try {
      CollectionAdminRequest.Delete req = new CollectionAdminRequest.Delete();
      req.setCollectionName(testCollectionName);
      req.process(cloudClient);
    } catch (Exception e) {
      // don't fail the test
      log.warn("Could not delete collection {} after test completed", testCollectionName);
    }
  }

  protected List<Replica> getActiveOrRecoveringReplicas(String testCollectionName, String shardId) throws Exception {    
    Map<String,Replica> activeReplicas = new HashMap<String,Replica>();    
    ZkStateReader zkr = cloudClient.getZkStateReader();
    ClusterState cs = zkr.getClusterState();
    assertNotNull(cs);
    for (Slice shard : cs.getActiveSlices(testCollectionName)) {
      if (shard.getName().equals(shardId)) {
        for (Replica replica : shard.getReplicas()) {
          String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
          if (ZkStateReader.ACTIVE.equals(replicaState) || ZkStateReader.RECOVERING.equals(replicaState)) {
            activeReplicas.put(replica.getName(), replica);
          }
        }
      }
    }        
    List<Replica> replicas = new ArrayList<Replica>();
    replicas.addAll(activeReplicas.values());
    return replicas;
  }  

  protected void assertDocsExistInAllReplicas(List<Replica> notLeaders,
      String testCollectionName, int firstDocId, int lastDocId)
      throws Exception {
    Replica leader =
        cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, "shard1", 10000);
    HttpSolrClient leaderSolr = getHttpSolrClient(leader, testCollectionName);
    List<HttpSolrClient> replicas =
        new ArrayList<HttpSolrClient>(notLeaders.size());

    for (Replica r : notLeaders) {
      replicas.add(getHttpSolrClient(r, testCollectionName));
    }
    try {
      for (int d = firstDocId; d <= lastDocId; d++) {
        String docId = String.valueOf(d);
        assertDocExists(leaderSolr, testCollectionName, docId);
        for (HttpSolrClient replicaSolr : replicas) {
          assertDocExists(replicaSolr, testCollectionName, docId);
        }
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (HttpSolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }
  
  protected HttpSolrClient getHttpSolrClient(Replica replica, String coll) throws Exception {
    ZkCoreNodeProps zkProps = new ZkCoreNodeProps(replica);
    String url = zkProps.getBaseUrl() + "/" + coll;
    return new HttpSolrClient(url);
  }
  
  protected void doSendDoc(int docid) throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setParam(UpdateRequest.MIN_REPFACT, String.valueOf(2));
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField(id, String.valueOf(docid));
    doc.addField("a_t", "hello" + docid);
    up.add(doc);
    int minAchievedRf =
        cloudClient.getMinAchievedReplicationFactor(cloudClient.getDefaultCollection(), cloudClient.request(up));
  }
  
  protected void sendDoc(int docId) throws Exception {
    try {
      doSendDoc(docId);
    } catch (SolrServerException e) {
      if (e.getRootCause() instanceof NoHttpResponseException) {
        // we don't know if the doc was accepted or not, we send again
        Thread.sleep(100);
        try {
          doSendDoc(docId);
        } catch (SolrServerException e2) {
          if (e2.getRootCause() instanceof NoHttpResponseException) {
            // we don't know if the doc was accepted or not, we send again
            Thread.sleep(3000);
            doSendDoc(docId);
          }
        }
      }
    }
  }
   
  /**
   * Query the real-time get handler for a specific doc by ID to verify it
   * exists in the provided server, using distrib=false so it doesn't route to another replica.
   */
  @SuppressWarnings("rawtypes")
  protected void assertDocExists(HttpSolrClient solr, String coll, String docId) throws Exception {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false"));
    NamedList rsp = solr.request(qr);
    String match = JSONTestUtil.matchObj("/id", rsp.get("doc"), new Integer(docId));
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL()
        + " due to: " + match + "; rsp="+rsp, match == null);
  }

  protected int getReplicaPort(Replica replica) {
    String replicaNode = replica.getNodeName();    
    String tmp = replicaNode.substring(replicaNode.indexOf(':')+1);
    if (tmp.indexOf('_') != -1)
      tmp = tmp.substring(0,tmp.indexOf('_'));
    return Integer.parseInt(tmp);    
  }

  protected void waitToSeeReplicasActive(String testCollectionName, String shardId, Set<String> replicasToCheck, int maxWaitSecs) throws Exception {
    long startMs = System.currentTimeMillis();

    ZkStateReader zkr = cloudClient.getZkStateReader();
    zkr.updateClusterState(true); // force the state to be fresh

    ClusterState cs = zkr.getClusterState();
    Collection<Slice> slices = cs.getActiveSlices(testCollectionName);
    boolean allReplicasUp = false;
    long waitMs = 0L;
    long maxWaitMs = maxWaitSecs * 1000L;
    while (waitMs < maxWaitMs && !allReplicasUp) {
      // refresh state every 2 secs
      if (waitMs % 2000 == 0)
        cloudClient.getZkStateReader().updateClusterState(true);

      cs = cloudClient.getZkStateReader().getClusterState();
      assertNotNull(cs);
      Slice shard = cs.getSlice(testCollectionName, shardId);
      assertNotNull("No Slice for "+shardId, shard);
      allReplicasUp = true; // assume true

      // wait to see all replicas are "active"
      for (Replica replica : shard.getReplicas()) {
        if (!replicasToCheck.contains(replica.getName()))
          continue;

        String replicaState = replica.getStr(ZkStateReader.STATE_PROP);
        if (!ZkStateReader.ACTIVE.equals(replicaState)) {
          log.info("Replica " + replica.getName() + " is currently " + replicaState);
          allReplicasUp = false;
        }
      }

      if (!allReplicasUp) {
        try {
          Thread.sleep(1000L);
        } catch (Exception ignoreMe) {}
        waitMs += 1000L;
      }
    } // end while

    if (!allReplicasUp)
      fail("Didn't see replicas "+ replicasToCheck +
          " come up within " + maxWaitMs + " ms! ClusterState: " + printClusterStateInfo(testCollectionName));

    long diffMs = (System.currentTimeMillis() - startMs);
    log.info("Took " + diffMs + " ms to see replicas ["+replicasToCheck+"] become active.");
  }
}
