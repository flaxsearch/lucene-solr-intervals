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

import static org.apache.solr.cloud.CollectionsAPIDistributedZkTest.setClusterProp;
import static org.apache.solr.common.cloud.ZkNodeProps.makeMap;

import java.net.URL;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;

//@Ignore("Not currently valid see SOLR-5580")
public class DeleteInactiveReplicaTest extends DeleteReplicaTest{

  @Override
  public void doTest() throws Exception {
    deleteInactiveReplicaTest();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  private void deleteInactiveReplicaTest() throws Exception {
    CloudSolrServer client = createCloudClient(null);

    String collectionName = "delDeadColl";

    setClusterProp(client, ZkStateReader.LEGACY_CLOUD, "false");
    
    createCollection(collectionName, client);
    
    waitForRecoveriesToFinish(collectionName, false);

    Thread.sleep(3000);

    boolean stopped = false;
    JettySolrRunner stoppedJetty = null;
    StringBuilder sb = new StringBuilder();
    Replica replica1 = null;
    Slice shard1 = null;
    long timeout = System.currentTimeMillis() + 3000;
    DocCollection testcoll = null;
    while(!stopped && System.currentTimeMillis()<timeout ) {
      testcoll = client.getZkStateReader().getClusterState().getCollection(collectionName);
      for (JettySolrRunner jetty : jettys)
        sb.append(jetty.getBaseUrl()).append(",");

      for (Slice slice : testcoll.getActiveSlices()) {
        for (Replica replica : slice.getReplicas())
          for (JettySolrRunner jetty : jettys) {
            URL baseUrl = null;
            try {
              baseUrl = jetty.getBaseUrl();
            } catch (Exception e) {
              continue;
            }
            if (baseUrl.toString().startsWith(
                replica.getStr(ZkStateReader.BASE_URL_PROP))) {
              stoppedJetty = jetty;
              ChaosMonkey.stop(jetty);
              replica1 = replica;
              shard1 = slice;
              stopped = true;
              break;
            }
          }
      }
      Thread.sleep(100);
    }


    if (!stopped) {
      fail("Could not find jetty to stop in collection " + testcoll
          + " jettys: " + sb);
    }
    
    long endAt = System.currentTimeMillis() + 3000;
    boolean success = false;
    while (System.currentTimeMillis() < endAt) {
      testcoll = client.getZkStateReader()
          .getClusterState().getCollection(collectionName);
      if (!"active".equals(testcoll.getSlice(shard1.getName())
          .getReplica(replica1.getName()).getStr(Slice.STATE))) {
        success = true;
      }
      if (success) break;
      Thread.sleep(100);
    }

    log.info("removed_replicas {}/{} ", shard1.getName(), replica1.getName());
    removeAndWaitForReplicaGone(collectionName, client, replica1,
        shard1.getName());
    ChaosMonkey.start(stoppedJetty);
    log.info("restarted jetty");

    Map m = makeMap("qt", "/admin/cores", "action", "status");

    SolrServer server = new HttpSolrServer(replica1.getStr(ZkStateReader.BASE_URL_PROP));
    NamedList<Object> resp = server.request(new QueryRequest(new MapSolrParams(m)));
    assertNull("The core is up and running again",
        ((NamedList) resp.get("status")).get(replica1.getStr("core")));
    server.shutdown();
    server = null;


    Exception exp = null;

    try {

      m = makeMap(
          "action", CoreAdminParams.CoreAdminAction.CREATE.toString(),
          ZkStateReader.COLLECTION_PROP, collectionName,
          ZkStateReader.SHARD_ID_PROP, "shard2",
          CoreAdminParams.NAME, "testcore");

      QueryRequest request = new QueryRequest(new MapSolrParams(m));
      request.setPath("/admin/cores");
      NamedList<Object> rsp = client.request(request);
    } catch (Exception e) {
      exp = e;
      log.info("error_expected",e);
    }
    assertNotNull( "Exception expected", exp);
    setClusterProp(client,ZkStateReader.LEGACY_CLOUD,null);
    client.shutdown();


  }
}
