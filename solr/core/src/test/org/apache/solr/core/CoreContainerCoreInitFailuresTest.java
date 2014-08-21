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

package org.apache.solr.core;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

public class CoreContainerCoreInitFailuresTest extends SolrTestCaseJ4 {
  
  File solrHome = null;
  CoreContainer cc = null;

  private void init(final String dirSuffix) {
    solrHome = createTempDir(dirSuffix);
  }

  @After
  public void cleanUp() throws Exception {
    if (cc != null) {
      cc.shutdown();
      cc = null;
    }

    solrHome = null;
  }

  public void testFlowWithEmpty() throws Exception {
    // reused state
    Map<String,CoreContainer.CoreLoadFailure> failures = null;
    Collection<String> cores = null;
    Exception fail = null;

    init("empty_flow");

    // solr.xml
    File solrXml = new File(solrHome, "solr.xml");
    FileUtils.write(solrXml, EMPTY_SOLR_XML, IOUtils.UTF_8);

    // ----
    // init the CoreContainer
    cc = new CoreContainer(solrHome.getAbsolutePath());
    cc.load();

    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 0, cores.size());
    
    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 0, failures.size());

    // -----
    // try to add a collection with a path that doesn't exist
    final CoreDescriptor bogus = new CoreDescriptor(cc, "bogus", "bogus_path");
    try {
      ignoreException(Pattern.quote("bogus_path"));
      cc.create(bogus);
      fail("bogus inst dir failed to trigger exception from create");
    } catch (SolrException e) {
      assertTrue("init exception doesn't mention bogus dir: " + e.getCause().getCause().getMessage(),
                 0 < e.getCause().getCause().getMessage().indexOf("bogus_path"));
      
    }
    
    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 0, cores.size());
    
    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("bogus").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getCause().getMessage(),
               0 < fail.getCause().getMessage().indexOf("bogus_path"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    try {
      SolrCore c = cc.getCore("bogus");
      fail("Failed to get Exception on accessing core with init failure");
    } catch (SolrException ex) {
      assertEquals(500, ex.code());
      // double wrapped
      String cause = ex.getCause().getCause().getMessage();
      assertTrue("getCore() ex cause doesn't mention init fail: " + cause,
                 0 < cause.indexOf("bogus_path"));
      
    }

    // let the test end here, with some recorded failures, and let cleanUp()
    // verify that there is no problem shuting down CoreContainer with known 
    // SolrCore failures
  }
  
  public void testFlowBadFromStart() throws Exception {

    // reused state
    Map<String,CoreContainer.CoreLoadFailure> failures = null;
    Collection<String> cores = null;
    Exception fail = null;

    init("bad_flow");

    // start with two collections: one valid, and one broken
    File solrXml = new File(solrHome, "solr.xml");
    FileUtils.write(solrXml, BAD_SOLR_XML, IOUtils.UTF_8);

    // our "ok" collection
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-defaults.xml"),
                       FileUtils.getFile(solrHome, "col_ok", "conf", "solrconfig.xml"));
    FileUtils.copyFile(getFile("solr/collection1/conf/schema-minimal.xml"),
                       FileUtils.getFile(solrHome, "col_ok", "conf", "schema.xml"));

    // our "bad" collection
    ignoreException(Pattern.quote("DummyMergePolicy"));
    FileUtils.copyFile(getFile("solr/collection1/conf/bad-mp-solrconfig.xml"),
                       FileUtils.getFile(solrHome, "col_bad", "conf", "solrconfig.xml"));
    FileUtils.copyFile(getFile("solr/collection1/conf/schema-minimal.xml"),
                       FileUtils.getFile(solrHome, "col_bad", "conf", "schema.xml"));


    // -----
    // init the  CoreContainer with the mix of ok/bad cores
    cc = new CoreContainer(solrHome.getAbsolutePath());
    cc.load();
    
    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 1, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    
    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("col_bad").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getMessage(),
               0 < fail.getMessage().indexOf("DummyMergePolicy"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    try {
      SolrCore c = cc.getCore("col_bad");
      fail("Failed to get Exception on accessing core with init failure");
    } catch (SolrException ex) {
      assertEquals(500, ex.code());
      // double wrapped
      String cause = ex.getCause().getCause().getMessage();
      assertTrue("getCore() ex cause doesn't mention init fail: " + cause,
                 0 < cause.indexOf("DummyMergePolicy"));
    }

    // -----
    // "fix" the bad collection
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-defaults.xml"),
                       FileUtils.getFile(solrHome, "col_bad", "conf", "solrconfig.xml"));
    final CoreDescriptor fixed = new CoreDescriptor(cc, "col_bad", "col_bad");
    cc.create(fixed);
    
    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));
    
    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 0, failures.size());


    // -----
    // try to add a collection with a path that doesn't exist
    final CoreDescriptor bogus = new CoreDescriptor(cc, "bogus", "bogus_path");
    try {
      ignoreException(Pattern.quote("bogus_path"));
      cc.create(bogus);
      fail("bogus inst dir failed to trigger exception from create");
    } catch (SolrException e) {
      assertTrue("init exception doesn't mention bogus dir: " + e.getCause().getCause().getMessage(),
                 0 < e.getCause().getCause().getMessage().indexOf("bogus_path"));
      
    }
    
    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));
    
    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("bogus").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getCause().getMessage(),
               0 < fail.getCause().getMessage().indexOf("bogus_path"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    try {
      SolrCore c = cc.getCore("bogus");
      fail("Failed to get Exception on accessing core with init failure");
    } catch (SolrException ex) {
      assertEquals(500, ex.code());
      // double wrapped
      String cause = ex.getCause().getCause().getMessage();
      assertTrue("getCore() ex cause doesn't mention init fail: " + cause,
                 0 < cause.indexOf("bogus_path"));
    }

    // -----
    // break col_bad's config and try to RELOAD to add failure

    final long col_bad_old_start = getCoreStartTime(cc, "col_bad");

    FileUtils.write
      (FileUtils.getFile(solrHome, "col_bad", "conf", "solrconfig.xml"),
       "This is giberish, not valid XML <", 
       IOUtils.UTF_8);

    try {
      ignoreException(Pattern.quote("SAX"));
      cc.reload("col_bad");
      fail("corrupt solrconfig.xml failed to trigger exception from reload");
    } catch (SolrException e) {
      Throwable rootException = getWrappedException(e);
      assertTrue("We're supposed to have a wrapped SAXParserException here, but we don't",
          rootException instanceof SAXParseException);
      SAXParseException se = (SAXParseException) rootException;
      assertTrue("reload exception doesn't refer to slrconfig.xml " + se.getSystemId(),
          0 < se.getSystemId().indexOf("solrconfig.xml"));

    }

    assertEquals("Failed core reload should not have changed start time",
                 col_bad_old_start, getCoreStartTime(cc, "col_bad"));

    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 2, failures.size());
    Throwable ex = getWrappedException(failures.get("col_bad").exception);
    assertNotNull("null failure for test core", ex);
    assertTrue("init failure isn't SAXParseException",
               ex instanceof SAXParseException);
    SAXParseException saxEx = (SAXParseException) ex;
    assertTrue("init failure doesn't mention problem: " + saxEx.toString(), saxEx.getSystemId().contains("solrconfig.xml"));

    // ----
    // fix col_bad's config (again) and RELOAD to fix failure
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-defaults.xml"),
                       FileUtils.getFile(solrHome, "col_bad", "conf", "solrconfig.xml"));
    cc.reload("col_bad");
    
    assertTrue("Core reload should have changed start time",
               col_bad_old_start < getCoreStartTime(cc, "col_bad"));
    

    // check that we have the cores we expect
    cores = cc.getCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());

  }

  private long getCoreStartTime(final CoreContainer cc, final String name) {
    try (SolrCore tmp = cc.getCore(name)) {
      return tmp.getStartTime();
    }
  }

  private static final String EMPTY_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\">\n" +
      "  </cores>\n" +
      "</solr>";

  private static final String BAD_SOLR_XML =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
    "<solr persistent=\"false\">\n" +
    "  <cores adminPath=\"/admin/cores\">\n" +
    "    <core name=\"col_ok\" instanceDir=\"col_ok\" />\n" + 
    "    <core name=\"col_bad\" instanceDir=\"col_bad\" />\n" + 
    "  </cores>\n" +
    "</solr>";
  
}
