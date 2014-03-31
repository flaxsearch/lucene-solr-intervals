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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;

import java.io.IOException;

public class TestRequestStatusCollectionAPI extends BasicDistributedZkTest {

  public TestRequestStatusCollectionAPI() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

  }

  public void doTest() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
    params.set("name", "collection2");
    params.set("numShards", 2);
    params.set("replicationFactor", 1);
    params.set("maxShardsPerNode", 100);
    params.set("collection.configName", "conf1");
    params.set("async", "1000");
    try {
      sendRequest(params);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Check for the request to be completed.
    int maxCounter = 10;

    NamedList r = null;
    NamedList status = null;
    String message = null;

    params = new ModifiableSolrParams();

    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(OverseerCollectionProcessor.REQUESTID, "1000");

    try {
      message = sendStatusRequestWithRetry(params, 10);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertEquals("Task 1000 not found in completed tasks.", "found 1000 in completed tasks", message);

    // Check for a random (hopefully non-existent request id
    params = new ModifiableSolrParams();
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(OverseerCollectionProcessor.REQUESTID, "9999999");
    try {
      r = sendRequest(params);
      status = (NamedList) r.get("status");
      message = (String) status.get("msg");
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertEquals("Task 9999999 found in tasks queue.", "Did not find taskid [9999999] in any tasks queue", message);

    params = new ModifiableSolrParams();
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.SPLITSHARD.toString());
    params.set("collection", "collection2");
    params.set("shard", "shard1");
    params.set("async", "1001");
    try {
      sendRequest(params);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Check for the request to be completed.
      params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
      params.set(OverseerCollectionProcessor.REQUESTID, "1001");
    try {
      message = sendStatusRequestWithRetry(params, maxCounter);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertEquals("Task 1001 not found in completed tasks.", "found 1001 in completed tasks", message);

      params = new ModifiableSolrParams();
      params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "collection2");
      params.set("numShards", 2);
      params.set("replicationFactor", 1);
      params.set("maxShardsPerNode", 100);
      params.set("collection.configName", "conf1");
      params.set("async", "1002");
    try {
      sendRequest(params);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }


    params = new ModifiableSolrParams();

      params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
      params.set(OverseerCollectionProcessor.REQUESTID, "1002");

    try {
      message = sendStatusRequestWithRetry(params, 10);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }


    assertEquals("Task 1002 not found in completed tasks.", "found 1002 in failed tasks", message);

      params = new ModifiableSolrParams();
      params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
      params.set("name", "collection3");
      params.set("numShards", 1);
      params.set("replicationFactor", 1);
      params.set("maxShardsPerNode", 100);
      params.set("collection.configName", "conf1");
      params.set("async", "1002");
    try {
      r = sendRequest(params);
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    assertEquals("Did not error out on duplicate requests (same request id)",
          "Task with the same requestid already exists.", r.get("error"));

  }


  /**
   * Helper method to send a status request with specific retry limit and return
   * the message/null from the success response.
   */
  private String sendStatusRequestWithRetry(ModifiableSolrParams params, int maxCounter)
      throws SolrServerException, IOException{
    NamedList status = null;
    String state = null;
    String message = null;
    NamedList r;
    while(maxCounter-- > 0) {
      r = sendRequest(params);
      status = (NamedList) r.get("status");
      state = (String) status.get("state");
      message = (String) status.get("msg");

      if(state.equals("completed") || state.equals("failed"))
        return (String) status.get("msg");

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {

      }

    }
    // Return last state?
    return message;
  }

  protected NamedList sendRequest(ModifiableSolrParams params) throws SolrServerException, IOException {
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrServer) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    HttpSolrServer baseServer = new HttpSolrServer(baseUrl);
    baseServer.setConnectionTimeout(15000);
    return baseServer.request(request);
  }
}
