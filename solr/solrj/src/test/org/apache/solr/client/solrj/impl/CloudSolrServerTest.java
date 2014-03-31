package org.apache.solr.client.solrj.impl;

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
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;


/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
public class CloudSolrServerTest extends AbstractFullDistribZkTestBase {
  
  private static final String SOLR_HOME = getFile("solrj" + File.separator + "solr").getAbsolutePath();

  @BeforeClass
  public static void beforeSuperClass() {
      AbstractZkTestCase.SOLRHOME = new File(SOLR_HOME());
  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  protected String getCloudSolrConfig() {
    return "solrconfig.xml";
  }
  
  @Override
  public String getSolrHome() {
    return SOLR_HOME;
  }
  
  public static String SOLR_HOME() {
    return SOLR_HOME;
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    // we expect this time of exception as shards go up and down...
    //ignoreException(".*");
    
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public CloudSolrServerTest() {
    super();
    sliceCount = 2;
    shardCount = 3;
  }
  
  @Override
  public void doTest() throws Exception {
    assertNotNull(cloudClient);
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(30);

    del("*:*");

    commit();
    
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField(id, "0");
    doc1.addField("a_t", "hello1");
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.addField(id, "2");
    doc2.addField("a_t", "hello2");
    
    UpdateRequest request = new UpdateRequest();
    request.add(doc1);
    request.add(doc2);
    request.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
    
    // Test single threaded routed updates for UpdateRequest
    NamedList response = cloudClient.request(request);
    CloudSolrServer.RouteResponse rr = (CloudSolrServer.RouteResponse) response;
    Map<String,LBHttpSolrServer.Req> routes = rr.getRoutes();
    Iterator<Map.Entry<String,LBHttpSolrServer.Req>> it = routes.entrySet()
        .iterator();
    while (it.hasNext()) {
      Map.Entry<String,LBHttpSolrServer.Req> entry = it.next();
      String url = entry.getKey();
      UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
          .getRequest();
      SolrInputDocument doc = updateRequest.getDocuments().get(0);
      String id = doc.getField("id").getValue().toString();
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add("q", "id:" + id);
      params.add("distrib", "false");
      QueryRequest queryRequest = new QueryRequest(params);
      HttpSolrServer solrServer = new HttpSolrServer(url);
      QueryResponse queryResponse = queryRequest.process(solrServer);
      SolrDocumentList docList = queryResponse.getResults();
      assertTrue(docList.getNumFound() == 1);
    }
    
    // Test the deleteById routing for UpdateRequest
    
    UpdateRequest delRequest = new UpdateRequest();
    delRequest.deleteById("0");
    delRequest.deleteById("2");
    delRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, false);
    cloudClient.request(delRequest);
    ModifiableSolrParams qParams = new ModifiableSolrParams();
    qParams.add("q", "*:*");
    QueryRequest qRequest = new QueryRequest(qParams);
    QueryResponse qResponse = qRequest.process(cloudClient);
    SolrDocumentList docs = qResponse.getResults();
    assertTrue(docs.getNumFound() == 0);
    
    // Test Multi-Threaded routed updates for UpdateRequest
    
    CloudSolrServer threadedClient = null;
    try {
      threadedClient = new CloudSolrServer(zkServer.getZkAddress());
      threadedClient.setParallelUpdates(true);
      threadedClient.setDefaultCollection("collection1");
      response = threadedClient.request(request);
      rr = (CloudSolrServer.RouteResponse) response;
      routes = rr.getRoutes();
      it = routes.entrySet()
          .iterator();
      while (it.hasNext()) {
        Map.Entry<String,LBHttpSolrServer.Req> entry = it.next();
        String url = entry.getKey();
        UpdateRequest updateRequest = (UpdateRequest) entry.getValue()
            .getRequest();
        SolrInputDocument doc = updateRequest.getDocuments().get(0);
        String id = doc.getField("id").getValue().toString();
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("q", "id:" + id);
        params.add("distrib", "false");
        QueryRequest queryRequest = new QueryRequest(params);
        HttpSolrServer solrServer = new HttpSolrServer(url);
        QueryResponse queryResponse = queryRequest.process(solrServer);
        SolrDocumentList docList = queryResponse.getResults();
        assertTrue(docList.getNumFound() == 1);
      }
    } finally {
      threadedClient.shutdown();
    }

    // Test that queries with _route_ params are routed by the client

    // Track request counts on each node before query calls
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection col = clusterState.getCollection(DEFAULT_COLLECTION);
    Map<String, Long> requestCountsMap = Maps.newHashMap();
    for (Slice slice : col.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);
        requestCountsMap.put(baseURL, getNumRequests(new HttpSolrServer(baseURL)));
      }
    }

    // Collect the base URLs of the replicas of shard that's expected to be hit
    DocRouter router = col.getRouter();
    Collection<Slice> expectedSlices = router.getSearchSlicesSingle("0", null, col);
    Set<String> expectedBaseURLs = Sets.newHashSet();
    for (Slice expectedSlice : expectedSlices) {
      for (Replica replica : expectedSlice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);
        expectedBaseURLs.add(baseURL);
      }
    }

    assertTrue("expected urls is not fewer than all urls! expected=" + expectedBaseURLs
        + "; all=" + requestCountsMap.keySet(),
        expectedBaseURLs.size() < requestCountsMap.size());

    // Calculate a number of shard keys that route to the same shard.
    int n;
    if (TEST_NIGHTLY) {
      n = random().nextInt(999) + 1;
    } else {
      n = random().nextInt(9) + 1;
    }
    
    List<String> sameShardRoutes = Lists.newArrayList();
    sameShardRoutes.add("0");
    for (int i = 1; i < n; i++) {
      String shardKey = Integer.toString(i);
      Collection<Slice> slices = router.getSearchSlicesSingle(shardKey, null, col);
      if (expectedSlices.equals(slices)) {
        sameShardRoutes.add(shardKey);
      }
    }

    assertTrue(sameShardRoutes.size() > 1);

    // Do N queries with _route_ parameter to the same shard
    for (int i = 0; i < n; i++) {
      ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set(CommonParams.Q, "*:*");
      solrParams.set(ShardParams._ROUTE_, sameShardRoutes.get(random().nextInt(sameShardRoutes.size())));
      cloudClient.query(solrParams);
    }

    // Request counts increase from expected nodes should aggregate to 1000, while there should be
    // no increase in unexpected nodes.
    int increaseFromExpectedUrls = 0;
    int increaseFromUnexpectedUrls = 0;
    Map<String, Long> numRequestsToUnexpectedUrls = Maps.newHashMap();
    for (Slice slice : col.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        String baseURL = (String) replica.get(ZkStateReader.BASE_URL_PROP);

        Long prevNumRequests = requestCountsMap.get(baseURL);
        Long curNumRequests = getNumRequests(new HttpSolrServer(baseURL));

        long delta = curNumRequests - prevNumRequests;
        if (expectedBaseURLs.contains(baseURL)) {
          increaseFromExpectedUrls += delta;
        } else {
          increaseFromUnexpectedUrls += delta;
          numRequestsToUnexpectedUrls.put(baseURL, delta);
        }
      }
    }

    assertEquals("Unexpected number of requests to expected URLs", n, increaseFromExpectedUrls);
    assertEquals("Unexpected number of requests to unexpected URLs: " + numRequestsToUnexpectedUrls,
        0, increaseFromUnexpectedUrls);

    del("*:*");
    commit();
  }

  private Long getNumRequests(HttpSolrServer solrServer) throws
      SolrServerException, IOException {
    HttpSolrServer server = new HttpSolrServer(solrServer.getBaseURL());
    server.setConnectionTimeout(15000);
    server.setSoTimeout(60000);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/mbeans");
    params.set("stats", "true");
    params.set("key", "org.apache.solr.handler.StandardRequestHandler");
    params.set("cat", "QUERYHANDLER");
    // use generic request to avoid extra processing of queries
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> resp = server.request(req);
    NamedList mbeans = (NamedList) resp.get("solr-mbeans");
    NamedList queryHandler = (NamedList) mbeans.get("QUERYHANDLER");
    NamedList select = (NamedList) queryHandler.get("org.apache.solr.handler.StandardRequestHandler");
    NamedList stats = (NamedList) select.get("stats");
    return (Long) stats.get("requests");
  }
  
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }
  
  public void testShutdown() throws MalformedURLException {
    CloudSolrServer server = new CloudSolrServer("[ff01::114]:33332");
    try {
      server.setZkConnectTimeout(100);
      server.connect();
      fail("Expected exception");
    } catch (SolrException e) {
      assertTrue(e.getCause() instanceof TimeoutException);
    } finally {
      server.shutdown();
    }
  }

}
