package org.apache.solr.handler;

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

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.SimplePostTool;
import org.junit.Test;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.solr.core.ConfigOverlay.getObjectByPath;

public class TestBlobHandler extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestBlobHandler.class);

  @Test
  public void doBlobHandlerTest() throws Exception {

    try (SolrClient client = createNewSolrClient("", getBaseUrl((HttpSolrClient) clients.get(0)))) {
      CollectionAdminResponse response1;
      CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
      createCollectionRequest.setCollectionName(".system");
      createCollectionRequest.setNumShards(1);
      createCollectionRequest.setReplicationFactor(2);
      response1 = createCollectionRequest.process(client);
      assertEquals(0, response1.getStatus());
      assertTrue(response1.isSuccess());
      DocCollection sysColl = cloudClient.getZkStateReader().getClusterState().getCollection(".system");
      Replica replica = sysColl.getActiveSlicesMap().values().iterator().next().getLeader();

      String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
      String url = baseUrl + "/.system/config/requestHandler";
      Map map = TestSolrConfigHandlerConcurrent.getAsMap(url, cloudClient);
      assertNotNull(map);
      assertEquals("solr.BlobHandler", getObjectByPath(map, true, Arrays.asList(
          "config",
          "requestHandler",
          "/blob",
          "class")));

      byte[] bytarr  = new byte[1024];
      for (int i = 0; i < bytarr.length; i++) bytarr[i]= (byte) (i % 127);
      byte[] bytarr2  = new byte[2048];
      for (int i = 0; i < bytarr2.length; i++) bytarr2[i]= (byte) (i % 127);
      postAndCheck(cloudClient, baseUrl, ByteBuffer.wrap( bytarr), 1);
      postAndCheck(cloudClient, baseUrl, ByteBuffer.wrap( bytarr2), 2);

      url = baseUrl + "/.system/blob/test/1";
      map = TestSolrConfigHandlerConcurrent.getAsMap(url,cloudClient);
      List l = (List) ConfigOverlay.getObjectByPath(map, false, Arrays.asList("response", "docs"));
      assertNotNull(""+map, l);
      assertTrue("" + map, l.size() > 0);
      map = (Map) l.get(0);
      assertEquals(""+bytarr.length,String.valueOf(map.get("size")));

      compareInputAndOutput(baseUrl+"/.system/blob/test?wt=filestream", bytarr2);
      compareInputAndOutput(baseUrl+"/.system/blob/test/1?wt=filestream", bytarr);
    }
  }

  public static void createSystemCollection(SolrClient client) throws SolrServerException, IOException {
    CollectionAdminResponse response1;
    CollectionAdminRequest.Create createCollectionRequest = new CollectionAdminRequest.Create();
    createCollectionRequest.setCollectionName(".system");
    createCollectionRequest.setNumShards(1);
    createCollectionRequest.setReplicationFactor(2);
    response1 = createCollectionRequest.process(client);
    assertEquals(0, response1.getStatus());
    assertTrue(response1.isSuccess());
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    System.clearProperty("numShards");
    System.clearProperty("zkHost");

    // insurance
    DirectUpdateHandler2.commitOnClose = true;
  }

  public static void postAndCheck(CloudSolrClient cloudClient, String baseUrl, ByteBuffer bytes, int count) throws Exception {
    postData(cloudClient, baseUrl, bytes);

    String url;
    Map map = null;
    List l;
    long start = System.currentTimeMillis();
    int i=0;
    for(;i<150;i++) {//15 secs
      url = baseUrl + "/.system/blob/test";
      map = TestSolrConfigHandlerConcurrent.getAsMap(url, cloudClient);
      String numFound = String.valueOf(ConfigOverlay.getObjectByPath(map, false, Arrays.asList("response", "numFound")));
      if(!(""+count).equals(numFound)) {
        Thread.sleep(100);
        continue;
      }
      l = (List) ConfigOverlay.getObjectByPath(map, false, Arrays.asList("response", "docs"));
      assertNotNull(l);
      map = (Map) l.get(0);
      assertEquals("" + bytes.limit(), String.valueOf(map.get("size")));
      return;
    }
    fail(MessageFormat.format("Could not successfully add blob after {0} attempts. Expecting {1} items. time elapsed {2}  output  for url is {3}",
        i,count, System.currentTimeMillis()-start,  getAsString(map)));
  }

  public static String getAsString(Map map) {
    return new String(ZkStateReader.toJSON(map), StandardCharsets.UTF_8);
  }

  private void compareInputAndOutput(String url, byte[] bytarr) throws IOException {

    HttpClient httpClient = cloudClient.getLbClient().getHttpClient();

    HttpGet httpGet = new HttpGet(url);
    HttpResponse entity = httpClient.execute(httpGet);
    ByteBuffer b = SimplePostTool.inputStreamToByteArray(entity.getEntity().getContent());
    try {
      assertEquals(b.limit(), bytarr.length);
      for (int i = 0; i < bytarr.length; i++) {
        assertEquals(b.get(i), bytarr[i]);
      }
    } finally {
      httpGet.releaseConnection();
    }

  }

  public static void postData(CloudSolrClient cloudClient, String baseUrl, ByteBuffer bytarr) throws IOException {
    HttpPost httpPost = null;
    HttpEntity entity;
    String response = null;
    try {
      httpPost = new HttpPost(baseUrl+"/.system/blob/test");
      httpPost.setHeader("Content-Type","application/octet-stream");
      httpPost.setEntity(new ByteArrayEntity(bytarr.array(), bytarr.arrayOffset(), bytarr.limit()));
      entity = cloudClient.getLbClient().getHttpClient().execute(httpPost).getEntity();
      try {
        response = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        Map m = (Map) ObjectBuilder.getVal(new JSONParser(new StringReader(response)));
        assertFalse("Error in posting blob "+ getAsString(m),m.containsKey("error"));
      } catch (JSONParser.ParseException e) {
        log.error(response);
        fail();
      }
    } finally {
      httpPost.releaseConnection();
    }
  }
}
