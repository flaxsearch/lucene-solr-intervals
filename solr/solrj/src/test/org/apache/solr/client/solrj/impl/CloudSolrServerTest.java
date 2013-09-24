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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
public class CloudSolrServerTest extends AbstractFullDistribZkTestBase {
  
  private static final String SOLR_HOME = ExternalPaths.SOURCE_HOME + File.separator + "solrj"
      + File.separator + "src" + File.separator + "test-files"
      + File.separator + "solrj" + File.separator + "solr";

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
    shardCount = 4;
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
    
    del("*:*");
    commit();
    
    indexr(id, 0, "a_t", "to come to the aid of their country.");
    
    CloudJettyRunner shard1Leader = shardToLeaderJetty.get("shard1");
    CloudJettyRunner shard2Leader = shardToLeaderJetty.get("shard2");
    
    if (cloudClient.isUpdatesToLeaders()) {
      // compare leaders list
      assertEquals(2, cloudClient.getLeaderUrlLists().get("collection1").size());
      HashSet<String> leaderUrlSet = new HashSet<String>();
      leaderUrlSet.addAll(cloudClient.getLeaderUrlLists().get("collection1"));
      assertTrue("fail check for leader:" + shard1Leader.url + " in "
          + leaderUrlSet, leaderUrlSet.contains(shard1Leader.url + "/"));
      assertTrue("fail check for leader:" + shard2Leader.url + " in "
          + leaderUrlSet, leaderUrlSet.contains(shard2Leader.url + "/"));
      
      // compare replicas list
      Set<String> replicas = new HashSet<String>();
      List<CloudJettyRunner> jetties = shardToJetty.get("shard1");
      for (CloudJettyRunner cjetty : jetties) {
        replicas.add(cjetty.url);
      }
      jetties = shardToJetty.get("shard2");
      for (CloudJettyRunner cjetty : jetties) {
        replicas.add(cjetty.url);
      }
      replicas.remove(shard1Leader.url);
      replicas.remove(shard2Leader.url);
      
      assertEquals(replicas.size(),
          cloudClient.getReplicasLists().get("collection1").size());
      
      for (String url : cloudClient.getReplicasLists().get("collection1")) {
        assertTrue("fail check for replica:" + url + " in " + replicas,
            replicas.contains(stripTrailingSlash(url)));
      }
    }
    
  }

  private String stripTrailingSlash(String url) {
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    }
    return url;
  }
  
  
  @Override
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = getDoc(fields);
    indexDoc(doc);
  }

}
