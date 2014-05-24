package org.apache.solr.cloud;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigSolr;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

public class SolrXmlInZkTest extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  protected ZkTestServer zkServer;

  protected String zkDir;

  private SolrZkClient zkClient;

  private ZkStateReader reader;

  private ConfigSolr cfg;

  @Before
  public void beforeClass() {
    System.setProperty("solr.solrxml.location", "zookeeper");
  }

  private void setUpZkAndDiskXml(boolean toZk, boolean leaveOnLocal) throws Exception {
    File tmpDir = createTempDir();
    recurseDelete(tmpDir);
    File solrHome = new File(tmpDir, "home");
    copyMinConf(new File(solrHome, "myCollect"));
    if (leaveOnLocal) {
      FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr-stress-new.xml"), new File(solrHome, "solr.xml"));
    }

    System.setProperty("solr.solr.home", solrHome.getAbsolutePath());

    ignoreException("No UpdateLog found - cannot sync");
    ignoreException("No UpdateLog found - cannot recover");

    System.setProperty("zkClientTimeout", "8000");

    zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper" + System.currentTimeMillis() + "/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    System.setProperty("zkHost", zkServer.getZkAddress());
    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(),
        zkServer.getZkAddress(), "solrconfig.xml", "schema.xml");

    zkClient = new SolrZkClient(zkServer.getZkAddress(), AbstractZkTestCase.TIMEOUT);

    if (toZk) {
      zkClient.makePath("solr.xml", XML_FOR_ZK.getBytes(StandardCharsets.UTF_8), true);
    }

    zkClient.close();

    log.info("####SETUP_START " + getTestName());

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    Method method = SolrDispatchFilter.class.getDeclaredMethod("loadConfigSolr", SolrResourceLoader.class);
    method.setAccessible(true);

    Object obj = method.invoke(new SolrDispatchFilter(), new SolrResourceLoader(null));
    cfg = (ConfigSolr) obj;

    log.info("####SETUP_END " + getTestName());
  }

  private void closeZK() throws Exception {
    if (zkClient != null) {
      zkClient.close();
    }

    if (reader != null) {
      reader.close();
    }
    zkServer.shutdown();
  }

  @Test
  public void testXmlOnBoth() throws Exception {
    try {
      setUpZkAndDiskXml(true, true);
      assertEquals("Should have gotten a new port the xml file sent to ZK, overrides the copy on disk",
          cfg.getZkHostPort(), "9045");
    } finally {
      closeZK();
    }
  }

  @Test
  public void testXmlInZkOnly() throws Exception {
    try {
      setUpZkAndDiskXml(true, false);
      assertEquals("Should have gotten a new port the xml file sent to ZK",
          cfg.getZkHostPort(), "9045");
    } finally {
      closeZK();
    }
  }

  @Test
  public void testNotInZkAndShouldBe() throws Exception {
    try {
      setUpZkAndDiskXml(false, true);
      fail("Should have gotten an exception here!");
    } catch (InvocationTargetException ite) {
      assertEquals("Should have an exception here, file not in ZK.",
          "Could not load solr.xml from zookeeper", ite.getTargetException().getMessage());
    } finally {
      closeZK();
    }
  }

  @Test
  public void testNotInZkOrOnDisk() throws Exception {
    try {
      System.clearProperty("solr.solrxml.location");
      System.setProperty("hostPort", "8787");
      setUpZkAndDiskXml(false, false); // solr.xml not on disk either
      fail("Should have thrown an exception here");
    } catch (InvocationTargetException ite) {
      assertTrue("Should be failing to create default solr.xml in code",
          ite.getTargetException().getCause().getMessage().indexOf("solr.xml does not exist") != -1);
    } finally {
      closeZK();
    }
  }

  @Test
  public void testOnDiskOnly() throws Exception {
    try {
      System.clearProperty("solr.solrxml.location");
      setUpZkAndDiskXml(false, true);
      assertEquals("Should have gotten the default port", cfg.getZkHostPort(), "8983");
    } finally {
      closeZK();
    }
  }

  @Test
  public void testBadSysProp() throws Exception {
    try {
      System.setProperty("solr.solrxml.location", "solrHomeDir");
      setUpZkAndDiskXml(false, true);
      fail("Should have thrown exception in SolrXmlInZkTest.testBadSysProp");
    } catch (InvocationTargetException ite) {
      assertEquals("Should have an exception in SolrXmlInZkTest.testBadSysProp, sysprop set to bogus value.",
          ite.getTargetException().getMessage(), "Bad solr.solrxml.location set: solrHomeDir - should be 'solrhome' or 'zookeeper'");
    } finally {
      closeZK();
    }

  }

  //SolrDispatchFilter.protected static ConfigSolr loadConfigSolr(SolrResourceLoader loader) {
  @Test
  public void testZkHostDiscovery() throws ClassNotFoundException, NoSuchMethodException,
      IllegalAccessException, InstantiationException, InvocationTargetException {

    // Should see an error when zkHost is not defined but solr.solrxml.location is set to zookeeper.
    System.clearProperty("zkHost");
    try {
      Method method = SolrDispatchFilter.class.getDeclaredMethod("loadConfigSolr", SolrResourceLoader.class);
      method.setAccessible(true);
      method.invoke(new SolrDispatchFilter(), new SolrResourceLoader(null));
      fail("Should have thrown an exception");
    } catch (InvocationTargetException ite) {
      assertTrue("Should be catching a SolrException", ite.getTargetException() instanceof SolrException);
      assertEquals("Caught Solr exception", ite.getTargetException().getMessage(),
          "Could not load solr.xml from zookeeper: zkHost system property not set");
    }
  }

  // Just a random port, I'm not going to use it but just check that the Solr instance constructed from the XML
  // file in ZK overrides the default port.
  private final String XML_FOR_ZK =
      "<solr>" +
          "  <solrcloud>" +
          "    <str name=\"host\">127.0.0.1</str>" +
          "    <int name=\"hostPort\">9045</int>" +
          "    <str name=\"hostContext\">${hostContext:solr}</str>" +
          "  </solrcloud>" +
          "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">" +
          "    <int name=\"socketTimeout\">${socketTimeout:120000}</int>" +
          "    <int name=\"connTimeout\">${connTimeout:15000}</int>" +
          "  </shardHandlerFactory>" +
          "</solr>";

}
