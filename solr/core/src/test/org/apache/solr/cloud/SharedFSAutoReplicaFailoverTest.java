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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

@Nightly
@Slow
@SuppressSSL
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class SharedFSAutoReplicaFailoverTest extends AbstractFullDistribZkTestBase {
  
  private static final boolean DEBUG = true;
  private static MiniDFSCluster dfsCluster;

  ThreadPoolExecutor executor = new ThreadPoolExecutor(0,
      Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
      new DefaultSolrThreadFactory("testExecutor"));
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;

  
  @BeforeClass
  public static void hdfsFailoverBeforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }
  
  @AfterClass
  public static void hdfsFailoverAfterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useJettyDataDir = false;
    System.setProperty("solr.xml.persist", "true");
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }

  
  public SharedFSAutoReplicaFailoverTest() {
    sliceCount = 2;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
    checkCreatedVsState = false;
    
  }

  @Test
  @ShardsFixed(num = 4)
  public void test() throws Exception {
    try {
      testBasics();
    } finally {
      if (DEBUG) {
        super.printLayout();
      }
    }
  }

  // very slow tests, especially since jetty is started and stopped
  // serially
  private void testBasics() throws Exception {
    String collection1 = "solrj_collection";
    Create createCollectionRequest = new Create();
    createCollectionRequest.setCollectionName(collection1);
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setReplicationFactor(2);
    createCollectionRequest.setMaxShardsPerNode(2);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterField("myOwnField");
    createCollectionRequest.setAutoAddReplicas(true);
    CollectionAdminResponse response = createCollectionRequest.process(cloudClient);

    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
    waitForRecoveriesToFinish(collection1, false);
    
    String collection2 = "solrj_collection2";
    createCollectionRequest = new Create();
    createCollectionRequest.setCollectionName(collection2);
    createCollectionRequest.setNumShards(2);
    createCollectionRequest.setReplicationFactor(2);
    createCollectionRequest.setMaxShardsPerNode(2);
    createCollectionRequest.setConfigName("conf1");
    createCollectionRequest.setRouterField("myOwnField");
    createCollectionRequest.setAutoAddReplicas(false);
    CollectionAdminResponse response2 = createCollectionRequest.process(getCommonCloudSolrClient());

    assertEquals(0, response2.getStatus());
    assertTrue(response2.isSuccess());
    
    waitForRecoveriesToFinish(collection2, false);

    ChaosMonkey.stop(jettys.get(1));
    ChaosMonkey.stop(jettys.get(2));

    Thread.sleep(3000);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLive(cloudClient.getZkStateReader(), collection1, 120000));

    assertSliceAndReplicaCount(collection1);

    assertEquals(4, getLiveAndActiveCount(collection1));
    assertTrue(getLiveAndActiveCount(collection2) < 4);

    ChaosMonkey.stop(jettys);
    ChaosMonkey.stop(controlJetty);

    assertTrue("Timeout waiting for all not live", ClusterStateUtil.waitForAllNotLive(cloudClient.getZkStateReader(), 45000));

    ChaosMonkey.start(jettys);
    ChaosMonkey.start(controlJetty);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLive(cloudClient.getZkStateReader(), collection1, 120000));

    assertSliceAndReplicaCount(collection1);

    int jettyIndex = random().nextInt(jettys.size());
    ChaosMonkey.stop(jettys.get(jettyIndex));
    ChaosMonkey.start(jettys.get(jettyIndex));

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLive(cloudClient.getZkStateReader(), collection1, 60000));

    //disable autoAddReplicas
    Map m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS,
        "val", "false");

    SolrRequest request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);

    int currentCount = getLiveAndActiveCount(collection1);

    ChaosMonkey.stop(jettys.get(3));

    //solr-no-core.xml has defined workLoopDelay=10s and waitAfterExpiration=10s
    //Hence waiting for 30 seconds to be on the safe side.
    Thread.sleep(30000);
    //Ensures that autoAddReplicas has not kicked in.
    assertTrue(currentCount > getLiveAndActiveCount(collection1));

    //enable autoAddReplicas
    m = makeMap(
        "action", CollectionParams.CollectionAction.CLUSTERPROP.toLower(),
        "name", ZkStateReader.AUTO_ADD_REPLICAS);

    request = new QueryRequest(new MapSolrParams(m));
    request.setPath("/admin/collections");
    cloudClient.request(request);

    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLive(cloudClient.getZkStateReader(), collection1, 60000));
    assertSliceAndReplicaCount(collection1);
  }

  private int getLiveAndActiveCount(String collection1) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection1);
    int liveAndActive = 0;
    for (Slice slice : slices) {
      for (Replica replica : slice.getReplicas()) {
        boolean live = cloudClient.getZkStateReader().getClusterState().liveNodesContain(replica.getNodeName());
        boolean active = replica.getStr(ZkStateReader.STATE_PROP).equals(ZkStateReader.ACTIVE);
        if (live && active) {
          liveAndActive++;
        }
      }
    }
    return liveAndActive;
  }

  private void assertSliceAndReplicaCount(String collection) {
    Collection<Slice> slices;
    slices = cloudClient.getZkStateReader().getClusterState().getActiveSlices(collection);
    assertEquals(2, slices.size());
    for (Slice slice : slices) {
      assertEquals(2, slice.getReplicas().size());
    }
  }
  
  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("solr.xml.persist");
  }
}
