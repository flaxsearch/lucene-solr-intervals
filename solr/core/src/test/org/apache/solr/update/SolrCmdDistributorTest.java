package org.apache.solr.update;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.LogDocMergePolicy;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.processor.DistributedUpdateProcessor;

import org.junit.BeforeClass;

public class SolrCmdDistributorTest extends BaseDistributedSearchTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {

    // we can't use the Randomized merge policy because the test depends on
    // being able to call optimize to have all deletes expunged.
    System.setProperty("solr.tests.mergePolicy", LogDocMergePolicy.class.getName());
  }
  private UpdateShardHandler updateShardHandler;
  
  public SolrCmdDistributorTest() {
    fixShardCount = true;
    shardCount = 4;
    stress = 0;
  }

  public static String getSchemaFile() {
    return "schema.xml";
  }
  
  public static  String getSolrConfigFile() {
    // use this because it has /update and is minimal
    return "solrconfig-tlog.xml";
  }
  
  // TODO: for now we redefine this method so that it pulls from the above
  // we don't get helpful override behavior due to the method being static
  @Override
  protected void createServers(int numShards) throws Exception {
    controlJetty = createJetty(new File(getSolrHome()), testDir + "/control/data", null, getSolrConfigFile(), getSchemaFile());

    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    shardsArr = new String[numShards];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner j = createJetty(new File(getSolrHome()),
          testDir + "/shard" + i + "/data", null, getSolrConfigFile(),
          getSchemaFile());
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      String shardStr = "127.0.0.1:" + j.getLocalPort() + context;
      shardsArr[i] = shardStr;
      sb.append(shardStr);
    }

    shards = sb.toString();
  }
  
  @Override
  public void doTest() throws Exception {
    del("*:*");
    
    SolrCmdDistributor cmdDistrib = new SolrCmdDistributor(5, updateShardHandler);
    
    ModifiableSolrParams params = new ModifiableSolrParams();

    List<Node> nodes = new ArrayList<Node>();

    ZkNodeProps nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        ((HttpSolrServer) controlClient).getBaseURL(),
        ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

    // add one doc to controlClient
    
    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = sdoc("id", 1);
    params = new ModifiableSolrParams();

    cmdDistrib.distribAdd(cmd, nodes, params);
    
    CommitUpdateCommand ccmd = new CommitUpdateCommand(null, false);
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    
    Response response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    long numFound = controlClient.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(1, numFound);
    
    HttpSolrServer client = (HttpSolrServer) clients.get(0);
    nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        client.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
    nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    
    // add another 2 docs to control and 3 to client
    cmdDistrib = new SolrCmdDistributor(5, updateShardHandler);
    cmd.solrDoc = sdoc("id", 2);
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribAdd(cmd, nodes, params);
    
    AddUpdateCommand cmd2 = new AddUpdateCommand(null);
    cmd2.solrDoc = sdoc("id", 3);

    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribAdd(cmd2, nodes, params);
    
    AddUpdateCommand cmd3 = new AddUpdateCommand(null);
    cmd3.solrDoc = sdoc("id", 4);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribAdd(cmd3, Collections.singletonList(nodes.get(1)), params);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();
    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    SolrDocumentList results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 3, numFound);
    
    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(3, numFound);
    
    // now delete doc 2 which is on both control and client1
    
    DeleteUpdateCommand dcmd = new DeleteUpdateCommand(null);
    dcmd.id = "2";
    
    

    cmdDistrib = new SolrCmdDistributor(5, updateShardHandler);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    
    cmdDistrib.distribDelete(dcmd, nodes, params);
    
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
    
    cmdDistrib.distribCommit(ccmd, nodes, params);
    cmdDistrib.finish();

    response = cmdDistrib.getResponse();
    
    assertEquals(response.errors.toString(), 0, response.errors.size());
    
    
    results = controlClient.query(new SolrQuery("*:*")).getResults();
    numFound = results.getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    numFound = client.query(new SolrQuery("*:*")).getResults()
        .getNumFound();
    assertEquals(results.toString(), 2, numFound);
    
    for (SolrServer c : clients) {
      c.optimize();
      //System.out.println(clients.get(0).request(new LukeRequest()));
    }
    
    int id = 5;
    
    cmdDistrib = new SolrCmdDistributor(5, updateShardHandler);
    
    int cnt = atLeast(303);
    for (int i = 0; i < cnt; i++) {
      nodes.clear();
      for (SolrServer c : clients) {
        if (random().nextBoolean()) {
          continue;
        }
        HttpSolrServer httpClient = (HttpSolrServer) c;
        nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
            httpClient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
        nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));

      }
      AddUpdateCommand c = new AddUpdateCommand(null);
      c.solrDoc = sdoc("id", id++);
      if (nodes.size() > 0) {
        params = new ModifiableSolrParams();
        cmdDistrib.distribAdd(c, nodes, params);
      }
    }
    
    nodes.clear();
    
    for (SolrServer c : clients) {
      HttpSolrServer httpClient = (HttpSolrServer) c;
      nodeProps = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
          httpClient.getBaseURL(), ZkStateReader.CORE_NAME_PROP, "");
      
      nodes.add(new StdNode(new ZkCoreNodeProps(nodeProps)));
    }
    
    final AtomicInteger commits = new AtomicInteger();
    for(JettySolrRunner jetty : jettys) {
      CoreContainer cores = ((SolrDispatchFilter) jetty.getDispatchFilter().getFilter()).getCores();
      SolrCore core = cores.getCore("collection1");
      try {
        core.getUpdateHandler().registerCommitCallback(new SolrEventListener() {
          @Override
          public void init(NamedList args) {}
          @Override
          public void postSoftCommit() {}
          @Override
          public void postCommit() {
            commits.incrementAndGet();
          }
          @Override
          public void newSearcher(SolrIndexSearcher newSearcher,
              SolrIndexSearcher currentSearcher) {}
        });
      } finally {
        core.close();
      }
    }
    params = new ModifiableSolrParams();
    params.set(DistributedUpdateProcessor.COMMIT_END_POINT, true);

    cmdDistrib.distribCommit(ccmd, nodes, params);
    
    cmdDistrib.finish();

    assertEquals(shardCount, commits.get());
    
    for (SolrServer c : clients) {
      NamedList<Object> resp = c.request(new LukeRequest());
      assertEquals("SOLR-3428: We only did adds - there should be no deletes",
          ((NamedList<Object>) resp.get("index")).get("numDocs"),
          ((NamedList<Object>) resp.get("index")).get("maxDoc"));
    }

  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    updateShardHandler = new UpdateShardHandler(10000, 10000);
  }
  
  @Override
  public void tearDown() throws Exception {
    updateShardHandler = null;
    super.tearDown();
  }
}
