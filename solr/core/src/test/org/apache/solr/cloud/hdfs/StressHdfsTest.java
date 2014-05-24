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

package org.apache.solr.cloud.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.cloud.ChaosMonkey;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@Slow
@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread(s)
public class StressHdfsTest extends BasicDistributedZkTest {

  private static final String DELETE_DATA_DIR_COLLECTION = "delete_data_dir";
  private static MiniDFSCluster dfsCluster;
  

  private boolean testRestartIntoSafeMode;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().getAbsolutePath());
    System.setProperty("solr.hdfs.home", dfsCluster.getURI().toString() + "/solr");
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    dfsCluster = null;
  }
  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
  
  public StressHdfsTest() {
    super();
    sliceCount = 1;
    shardCount = TEST_NIGHTLY ? 7 : random().nextInt(2) + 1;
    testRestartIntoSafeMode = random().nextBoolean();
  }
  
  protected String getSolrXml() {
    return "solr-no-core.xml";
  }
  
  @Override
  public void doTest() throws Exception {
    int cnt = random().nextInt(2) + 1;
    for (int i = 0; i < cnt; i++) {
      createAndDeleteCollection();
    }

    if (testRestartIntoSafeMode) {
      Timer timer = new Timer();
      
      try {
        createCollection(DELETE_DATA_DIR_COLLECTION, 1, 1, 1);
        
        waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
        
        ChaosMonkey.stop(jettys.get(0));
        
        // enter safe mode and restart a node
        NameNodeAdapter.enterSafeMode(dfsCluster.getNameNode(), false);
        
        int rnd = LuceneTestCase.random().nextInt(10000);
        
        timer.schedule(new TimerTask() {
          
          @Override
          public void run() {
            NameNodeAdapter.leaveSafeMode(dfsCluster.getNameNode());
          }
        }, rnd);
        
        ChaosMonkey.start(jettys.get(0));
        
        waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
      } finally {
        timer.cancel();
      }
    }
  }

  private void createAndDeleteCollection() throws SolrServerException,
      IOException, Exception, KeeperException, InterruptedException,
      URISyntaxException {
    
    boolean overshard = random().nextBoolean();
    int rep;
    int nShards;
    int maxReplicasPerNode;
    if (overshard) {
      nShards = shardCount * 2;
      maxReplicasPerNode = 8;
      rep = 1;
    } else {
      nShards = shardCount / 2;
      maxReplicasPerNode = 1;
      rep = 2;
      if (nShards == 0) nShards = 1;
    }
    
    createCollection(DELETE_DATA_DIR_COLLECTION, nShards, rep, maxReplicasPerNode);

    waitForRecoveriesToFinish(DELETE_DATA_DIR_COLLECTION, false);
    cloudClient.setDefaultCollection(DELETE_DATA_DIR_COLLECTION);
    cloudClient.getZkStateReader().updateClusterState(true);
    
    for (int i = 1; i < nShards + 1; i++) {
      cloudClient.getZkStateReader().getLeaderRetry(DELETE_DATA_DIR_COLLECTION, "shard" + i, 30000);
    }
    
    // collect the data dirs
    List<String> dataDirs = new ArrayList<>();
    
    int i = 0;
    for (SolrServer client : clients) {
      HttpSolrServer c = new HttpSolrServer(getBaseUrl(client)
          + "/delete_data_dir");
      try {
        c.add(getDoc("id", i++));
        if (random().nextBoolean()) c.add(getDoc("id", i++));
        if (random().nextBoolean()) c.add(getDoc("id", i++));
        if (random().nextBoolean()) {
          c.commit();
        } else {
          c.commit(true, true, true);
        }
        
        c.query(new SolrQuery("id:" + i));
        c.setConnectionTimeout(30000);
        NamedList<Object> response = c.query(
            new SolrQuery().setRequestHandler("/admin/system")).getResponse();
        NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
        String dataDir = (String) ((NamedList<Object>) coreInfo.get("directory")).get("data");
        dataDirs.add(dataDir);
      } finally {
        c.shutdown();
      }
    }
    
    if (random().nextBoolean()) {
      cloudClient.deleteByQuery("*:*");
      cloudClient.commit();
      
      assertEquals(0, cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.DELETE.toString());
    params.set("name", DELETE_DATA_DIR_COLLECTION);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");
    cloudClient.request(request);
    
    long timeout = System.currentTimeMillis() + 10000;
    while (cloudClient.getZkStateReader().getClusterState().hasCollection(DELETE_DATA_DIR_COLLECTION)) {
      if (System.currentTimeMillis() > timeout) {
        throw new AssertionError("Timeout waiting to see removed collection leave clusterstate");
      }
      
      Thread.sleep(200);
      cloudClient.getZkStateReader().updateClusterState(true);
    }
    
    // check that all dirs are gone
    for (String dataDir : dataDirs) {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.newInstance(new URI(dataDir), conf);
      assertFalse(
          "Data directory exists after collection removal : " + dataDir,
          fs.exists(new Path(dataDir)));
      fs.close();
    }
  }
}
