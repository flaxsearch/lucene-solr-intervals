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
package org.apache.solr.schema;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.*;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

public class TestBinaryField extends LuceneTestCase {
  HttpSolrServer server;
  JettySolrRunner jetty;

  int port = 0;
  static final String context = "/example";

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  @Override
  public void setUp() throws Exception {
    super.setUp();

    File home = new File(TEMP_DIR,
        "solrtest-TestBinaryField-" + System.currentTimeMillis());

    File homeDir = new File(home, "example");
    File dataDir = new File(homeDir + "/collection1", "data");
    File confDir = new File(homeDir + "/collection1", "conf");

    homeDir.mkdirs();
    dataDir.mkdirs();
    confDir.mkdirs();

    SolrResourceLoader loader = new SolrResourceLoader("solr/collection1");
    File f = new File(confDir, "solrconfig.xml");
    String fname = "solr/collection1/conf/solrconfig-slave1.xml";
    FileOutputStream out = new FileOutputStream(f);
    IOUtils.copy(loader.openResource(fname), out);
    out.close();
    f = new File(confDir, "schema.xml");
    fname = "solr/collection1/conf/schema-binaryfield.xml";
    out = new FileOutputStream(f);
    IOUtils.copy(loader.openResource(fname), out);
    out.close();
    System.setProperty("solr.data.dir", dataDir.getAbsolutePath());
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));

    jetty = new JettySolrRunner(homeDir.getAbsolutePath(), context, 0);
    jetty.start();
    port = jetty.getLocalPort();

    String url = "http://127.0.0.1:" + jetty.getLocalPort() + context;
    server = new HttpSolrServer(url);
  }

  public void testSimple() throws Exception {
    byte[] buf = new byte[10];
    for (int i = 0; i < 10; i++) {
      buf[i] = (byte) i;
    }
    SolrInputDocument doc = null;
    doc = new SolrInputDocument();
    doc.addField("id", 1);
    doc.addField("data", ByteBuffer.wrap(buf, 2, 5));
    server.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 2);
    doc.addField("data", ByteBuffer.wrap(buf, 4, 3));
    server.add(doc);

    doc = new SolrInputDocument();
    doc.addField("id", 3);
    doc.addField("data", buf);
    server.add(doc);

    server.commit();

    QueryResponse resp = server.query(new SolrQuery("*:*"));
    SolrDocumentList res = resp.getResults();
    List<Bean> beans = resp.getBeans(Bean.class);
    assertEquals(3, res.size());
    assertEquals(3, beans.size());
    for (SolrDocument d : res) {
      Integer id = (Integer) d.getFieldValue("id");
      byte[] data = (byte[]) d.getFieldValue("data");
      if (id == 1) {
        assertEquals(5, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)(i + 2), b);
        }

      } else if (id == 2) {
        assertEquals(3, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)(i + 4), b);
        }


      } else if (id == 3) {
        assertEquals(10, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)i, b);
        }

      }

    }
    for (Bean d : beans) {
      Integer id = d.id;
      byte[] data = d.data;
      if (id == 1) {
        assertEquals(5, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)(i + 2), b);
        }

      } else if (id == 2) {
        assertEquals(3, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)(i + 4), b);
        }


      } else if (id == 3) {
        assertEquals(10, data.length);
        for (int i = 0; i < data.length; i++) {
          byte b = data[i];
          assertEquals((byte)i, b);
        }

      }

    }

  }
  public static class Bean{
    @Field
    int id;
    @Field
    byte [] data;
  }


  @Override
  public void tearDown() throws Exception {
    jetty.stop();
    System.clearProperty("tests.shardhandler.randomSeed");
    super.tearDown();
  }
}
