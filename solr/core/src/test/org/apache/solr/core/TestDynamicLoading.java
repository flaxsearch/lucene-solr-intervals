package org.apache.solr.core;

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


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.handler.TestBlobHandler;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SimplePostTool;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TestDynamicLoading extends AbstractFullDistribZkTestBase {
  static final Logger log =  LoggerFactory.getLogger(TestDynamicLoading.class);
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private void setupHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(new RESTfulServerProvider() {
        @Override
        public String getBaseURL() {
          return ((HttpSolrClient)client).getBaseURL();
        }
      });
      restTestHarnesses.add(harness);
    }
  }

  @Override
  public void distribTearDown() throws Exception {
    super.distribTearDown();
    for (RestTestHarness r : restTestHarnesses) {
      r.close();
    }
  }

  @Test
  public void testDynamicLoading() throws Exception {
    setupHarnesses();
    String payload = "{\n" +
        "'create-requesthandler' : { 'name' : '/test1', 'class': 'org.apache.solr.core.BlobStoreTestRequestHandler' , 'lib':'test','version':'1'}\n" +
        "}";
    RestTestHarness client = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    TestSolrConfigHandler.runConfigCommand(client,"/config?wt=json",payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay?wt=json",
        null,
        Arrays.asList("overlay", "requestHandler", "/test1", "lib"),
        "test",10);

    Map map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);

    assertNotNull(map = (Map) map.get("error"));
    assertEquals(".system collection not available", map.get("msg"));

    HttpSolrClient randomClient = (HttpSolrClient) clients.get(random().nextInt(clients.size()));
    String baseURL = randomClient.getBaseURL();
    baseURL = baseURL.substring(0, baseURL.lastIndexOf('/'));
    TestBlobHandler.createSystemCollection(new HttpSolrClient(baseURL, randomClient.getHttpClient()));
    waitForRecoveriesToFinish(".system", true);

    map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);

    assertNotNull(map = (Map) map.get("error"));
    assertEquals("no such blob or version available: test/1", map.get("msg"));
    ByteBuffer jar = generateZip( TestDynamicLoading.class,BlobStoreTestRequestHandler.class);
    TestBlobHandler.postAndCheck(cloudClient, baseURL, jar,1);

    boolean success= false;
    for(int i=0;i<50;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);
      if(BlobStoreTestRequestHandler.class.getName().equals(map.get("class"))){
        success = true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue(new String( ZkStateReader.toJSON(map) , StandardCharsets.UTF_8), success );

    jar = generateZip( TestDynamicLoading.class,BlobStoreTestRequestHandlerV2.class);
    TestBlobHandler.postAndCheck(cloudClient, baseURL, jar,2);

    payload = " {\n" +
        "  'set' : {'watched': {" +
        "                    'x':'X val',\n" +
        "                    'y': 'Y val'}\n" +
        "             }\n" +
        "  }";

    TestSolrConfigHandler.runConfigCommand(client,"/config/params?wt=json",payload);
    TestSolrConfigHandler.testForResponseElement(
        client,
        null,
        "/config/params?wt=json",
        cloudClient,
        Arrays.asList("response", "params", "watched", "x"),
        "X val",
        10);


    payload = "{\n" +
        "'update-requesthandler' : { 'name' : '/test1', 'class': 'org.apache.solr.core.BlobStoreTestRequestHandlerV2' , 'lib':'test','version':2}\n" +
        "}";

    client = restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
    TestSolrConfigHandler.runConfigCommand(client,"/config?wt=json",payload);
    TestSolrConfigHandler.testForResponseElement(client,
        null,
        "/config/overlay?wt=json",
        null,
        Arrays.asList("overlay", "requestHandler", "/test1", "version"),
        2l,10);

    success= false;
    for(int i=0;i<100;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);
      if(BlobStoreTestRequestHandlerV2.class.getName().equals(map.get("class"))) {
        success = true;
        break;
      }
      Thread.sleep(100);
    }

    assertTrue("New version of class is not loaded " + new String(ZkStateReader.toJSON(map), StandardCharsets.UTF_8), success);

    for(int i=0;i<100;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);
      if("X val".equals(map.get("x"))){
         success = true;
         break;
      }
      Thread.sleep(100);
    }

    payload = " {\n" +
        "  'set' : {'watched': {" +
        "                    'x':'X val changed',\n" +
        "                    'y': 'Y val'}\n" +
        "             }\n" +
        "  }";

    TestSolrConfigHandler.runConfigCommand(client,"/config/params?wt=json",payload);
    for(int i=0;i<50;i++) {
      map = TestSolrConfigHandler.getRespMap("/test1?wt=json", client);
      if("X val changed".equals(map.get("x"))){
        success = true;
        break;
      }
      Thread.sleep(100);
    }
    assertTrue("listener did not get triggered" + new String(ZkStateReader.toJSON(map), StandardCharsets.UTF_8), success);


  }


  public static ByteBuffer generateZip(Class... classes) throws IOException {
    ZipOutputStream zipOut = null;
    SimplePostTool.BAOS bos = new SimplePostTool.BAOS();
    zipOut = new ZipOutputStream(bos);
    zipOut.setLevel(ZipOutputStream.DEFLATED);
    for (Class c : classes) {
      String path = c.getName().replace('.', '/').concat(".class");
      ZipEntry entry = new ZipEntry(path);
      ByteBuffer b = SimplePostTool.inputStreamToByteArray(c.getClassLoader().getResourceAsStream(path));
      zipOut.putNextEntry(entry);
      zipOut.write(b.array(), 0, b.limit());
      zipOut.closeEntry();
    }
    zipOut.close();
    return bos.getByteBuffer();
  }

}
