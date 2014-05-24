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

package org.apache.solr.velocity;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.VelocityResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringWriter;

public class VelocityResponseWriterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml", getFile("velocity/solr").getAbsolutePath());
  }

  @Test
  public void testCustomParamTemplate() throws Exception {
    // This test doesn't use the Solr core, just the response writer directly

    org.apache.solr.response.VelocityResponseWriter vrw = new VelocityResponseWriter();
    SolrQueryRequest req = req("v.template","custom", "v.template.custom","$response.response.response_data");
    SolrQueryResponse rsp = new SolrQueryResponse();
    StringWriter buf = new StringWriter();
    rsp.add("response_data", "testing");
    vrw.write(buf, req, rsp);
    assertEquals("testing", buf.toString());
  }

  @Test
  public void testVelocityResponseWriterRegistered() {
    QueryResponseWriter writer = h.getCore().getQueryResponseWriter("velocity");

    assertTrue("VrW registered check", writer instanceof VelocityResponseWriter);
  }

  @Test
  public void testSolrResourceLoaderTemplate() throws Exception {
    assertEquals("0", h.query(req("q","*:*", "wt","velocity","v.template","numFound")));
  }
}
