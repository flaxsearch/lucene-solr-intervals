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

package org.apache.solr.client.solrj.impl;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.SSLTestConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class BasicHttpSolrClientTest extends SolrJettyTestBase {
  
  public static class RedirectServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.sendRedirect("/solr/collection1/select?" + req.getQueryString());
    }
  }
  
  public static class SlowServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {}
    }
  }
  
  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      queryString = null;
    }
    
    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String,String> headers = null;
    public static Map<String,String[]> parameters = null;
    public static String queryString = null;
    
    public static void setErrorCode(Integer code) {
      errorCode = code;
    }
    

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "get";
      recordRequest(req, resp);
    }
    
    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name, req.getHeader(name));
      }
    }

    private void setParameters(HttpServletRequest req) {
      parameters = req.getParameterMap();
    }

    private void setQueryString(HttpServletRequest req) {
      queryString = req.getQueryString();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "post";
      recordRequest(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "put";
      recordRequest(req, resp);
    }
    
    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      setParameters(req);
      setQueryString(req);
      if (null != errorCode) {
        try { 
          resp.sendError(errorCode); 
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in DebugServlet", e);
        }
      }
    }
  }
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(legacyExampleCollection1SolrHome(), null, null);
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(RedirectServlet.class, "/redirect/*");
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(SlowServlet.class, "/slow/*");
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(DebugServlet.class, "/debug/*");
  }
  
  @Test
  public void testTimeout() throws Exception {

    SolrQuery q = new SolrQuery("*:*");
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/slow/foo")) {
      client.setSoTimeout(2000);
      QueryResponse response = client.query(q, METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("Timeout"));
    }

  }
  
  /**
   * test that SolrExceptions thrown by HttpSolrClient can
   * correctly encapsulate http status codes even when not on the list of
   * ErrorCodes solr may return.
   */
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(status + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
                 ErrorCode.UNKNOWN, ErrorCode.getErrorCode(status));

    try ( HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      DebugServlet.setErrorCode(status);
      try {
        SolrQuery q = new SolrQuery("foo");
        client.query(q, METHOD.GET);
        fail("Didn't get excepted exception from oversided request");
      } catch (SolrException e) {
        System.out.println(e);
        assertEquals("Unexpected exception status code", status, e.code());
      }
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery() throws IOException {
    DebugServlet.clear();
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");
      try {
        client.query(q, METHOD.GET);
      } catch (Throwable t) {
      }

      //default method
      assertEquals("get", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      //content-type
      assertEquals(null, DebugServlet.headers.get("Content-Type"));
      //param encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //POST
      DebugServlet.clear();
      try {
        client.query(q, METHOD.POST);
      } catch (Throwable t) {
      }
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      //PUT
      DebugServlet.clear();
      try {
        client.query(q, METHOD.PUT);
      } catch (Throwable t) {
      }
      assertEquals("put", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      //XML/GET
      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      try {
        client.query(q, METHOD.GET);
      } catch (Throwable t) {
      }
      assertEquals("get", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      //XML/POST
      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      try {
        client.query(q, METHOD.POST);
      } catch (Throwable t) {
      }
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      try {
        client.query(q, METHOD.PUT);
      } catch (Throwable t) {
      }
      assertEquals("put", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
    }

  }

  @Test
  public void testDelete() throws IOException {
    DebugServlet.clear();
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      try {
        client.deleteById("id");
      } catch (Throwable t) {
      }

      //default method
      assertEquals("post", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      //XML
      client.setParser(new XMLResponseParser());
      try {
        client.deleteByQuery("*:*");
      } catch (Throwable t) {
      }

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    }

  }
  
  @Test
  public void testUpdate() throws IOException {
    DebugServlet.clear();
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      UpdateRequest req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setParam("a", "\u1234");
      try {
        client.request(req);
      } catch (Throwable t) {
      }

      //default method
      assertEquals("post", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //content type
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
      //parameter encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //XML response
      client.setParser(new XMLResponseParser());
      try {
        client.request(req);
      } catch (Throwable t) {
      }
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //javabin request
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());
      DebugServlet.clear();
      try {
        client.request(req);
      } catch (Throwable t) {
      }
      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }

  }
  
  @Test
  public void testRedirect() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/redirect/foo")) {
      SolrQuery q = new SolrQuery("*:*");
      // default = false
      try {
        QueryResponse response = client.query(q);
        fail("Should have thrown an exception.");
      } catch (SolrServerException e) {
        assertTrue(e.getMessage().contains("redirect"));
      }
      client.setFollowRedirects(true);
      try {
        QueryResponse response = client.query(q);
      } catch (Throwable t) {
        fail("Exception was thrown:" + t);
      }
      //And back again:
      client.setFollowRedirects(false);
      try {
        QueryResponse response = client.query(q);
        fail("Should have thrown an exception.");
      } catch (SolrServerException e) {
        assertTrue(e.getMessage().contains("redirect"));
      }
    }

  }
  
  @Test
  public void testCompression() throws Exception {
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("*:*");
      
      // verify request header gets set
      DebugServlet.clear();
      try {
        client.query(q);
      } catch (Throwable t) {}
      assertNull(DebugServlet.headers.get("Accept-Encoding"));
      client.setAllowCompression(true);
      try {
        client.query(q);
      } catch (Throwable t) {}
      assertNotNull(DebugServlet.headers.get("Accept-Encoding"));
      client.setAllowCompression(false);
      try {
        client.query(q);
      } catch (Throwable t) {}
      assertNull(DebugServlet.headers.get("Accept-Encoding"));
    }
    
    // verify server compresses output
    HttpGet get = new HttpGet(jetty.getBaseUrl().toString() + "/collection1" +
                              "/select?q=foo&wt=xml");
    get.setHeader("Accept-Encoding", "gzip");
    CloseableHttpClient httpclient = HttpClientUtil.createClient(null);
    HttpEntity entity = null;
    try {
      HttpResponse response = httpclient.execute(get);
      entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      assertEquals("gzip", ceheader.getValue());
      
    } finally {
      if(entity!=null) {
        entity.getContent().close();
      }
      httpclient.close();
    }
    
    // verify compressed response can be handled
    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/collection1")) {
      client.setAllowCompression(true);
      SolrQuery q = new SolrQuery("foo");
      QueryResponse response = client.query(q);
      assertEquals(0, response.getStatus());
    }
  }
  
  @Test
  public void testSetParametersExternalClient() throws IOException{

    try (CloseableHttpClient httpClient = HttpClientUtil.createClient(null);
         HttpSolrClient solrClient = new HttpSolrClient(jetty.getBaseUrl().toString(), httpClient)) {

      try {
        solrClient.setMaxTotalConnections(1);
        fail("Operation should not succeed.");
      } catch (UnsupportedOperationException e) {}
      try {
        solrClient.setDefaultMaxConnectionsPerHost(1);
        fail("Operation should not succeed.");
      } catch (UnsupportedOperationException e) {}

    }
  }

  @Test
  public void testGetRawStream() throws SolrServerException, IOException{
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    try {
      HttpSolrClient solrClient = new HttpSolrClient(jetty.getBaseUrl().toString() + "/collection1",
                                                 client, null);
      QueryRequest req = new QueryRequest();
      NamedList response = solrClient.request(req);
      InputStream stream = (InputStream)response.get("stream");
      assertNotNull(stream);
      stream.close();
    } finally {
      client.close();
    }
  }

  /**
   * A trivial test that verifies the example keystore used for SSL testing can be 
   * found using the base class. this helps future-proof against the possibility of 
   * something moving/breaking the keystore path in a way that results in the SSL 
   * randomization logic being forced to silently never use SSL.  (We can't enforce 
   * this type of check in the base class because then it would not be usable by client 
   * code depending on the test framework
   */
  public void testExampleKeystorePath() {
    assertNotNull("Example keystore is null, meaning that something has changed in the " +
                  "structure of the example configs and/or ExternalPaths.java - " + 
                  "SSL randomization is broken",
                  SSLTestConfig.TEST_KEYSTORE);
  }


  private int findUnusedPort() {
    for (int port = 0; port < 65535; port++) {
      Socket s = new Socket();
      try {
        s.bind(null);
        int availablePort = s.getLocalPort();
        s.close();
        return availablePort;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    throw new RuntimeException("Could not find unused TCP port.");
  }

  private Set<String> setOf(String... keys) {
    Set<String> set = new TreeSet<String>();
    if (keys != null) {
      for (String k : keys) {
        set.add(k);
      }
    }
    return set;
  }

  private void setReqParamsOf(UpdateRequest req, String... keys) {
    if (keys != null) {
      for (String k : keys) {
        req.setParam(k, k+"Value");
      }
    }
  }

  private void verifyServletState(HttpSolrClient client, SolrRequest request) {
    // check query String
    Iterator<String> paramNames = request.getParams().getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String name = paramNames.next();
      String [] values = request.getParams().getParams(name);
      if (values != null) {
        for (String value : values) {
          boolean shouldBeInQueryString = client.getQueryParams().contains(name)
            || (request.getQueryParams() != null && request.getQueryParams().contains(name));
          assertEquals(shouldBeInQueryString, DebugServlet.queryString.contains(name + "=" + value));
          // in either case, it should be in the parameters
          assertNotNull(DebugServlet.parameters.get(name));
          assertEquals(1, DebugServlet.parameters.get(name).length);
          assertEquals(value, DebugServlet.parameters.get(name)[0]);
        }
      }
    }
  }

  @Test
  public void testQueryString() throws Exception {

    try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      // test without request query params
      DebugServlet.clear();
      client.setQueryParams(setOf("serverOnly"));
      UpdateRequest req = new UpdateRequest();
      setReqParamsOf(req, "serverOnly", "notServer");
      try {
        client.request(req);
      } catch (Throwable t) {}
      verifyServletState(client, req);
  
      // test without server query params
      DebugServlet.clear();
      client.setQueryParams(setOf());
      req = new UpdateRequest();
      req.setQueryParams(setOf("requestOnly"));
      setReqParamsOf(req, "requestOnly", "notRequest");
      try {
        client.request(req);
      } catch (Throwable t) {}
      verifyServletState(client, req);
  
      // test with both request and server query params
      DebugServlet.clear();
      req = new UpdateRequest();
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
       try {
        client.request(req);
      } catch (Throwable t) {}
      verifyServletState(client, req);
  
      // test with both request and server query params with single stream
      DebugServlet.clear();
      req = new UpdateRequest();
      req.add(new SolrInputDocument());
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
       try {
        client.request(req);
      } catch (Throwable t) {}
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request
      // so it passes the verification step.
      req.setQueryParams(setOf("requestOnly", "both", "neither"));
      verifyServletState(client, req);
    }
  }
}
