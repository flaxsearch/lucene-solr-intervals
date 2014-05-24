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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestSolrXmlPersistor  extends SolrTestCaseJ4 {

  private static final List<CoreDescriptor> EMPTY_CD_LIST = ImmutableList.<CoreDescriptor>builder().build();

  @Test
  public void selfClosingCoresTagIsPersisted() {

    final String solrxml = "<solr><cores adminHandler=\"/admin\"/></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals("<solr><cores adminHandler=\"/admin\"></cores></solr>", persistor.buildSolrXML(EMPTY_CD_LIST));

  }

  @Test
  public void emptyCoresTagIsPersisted() {
    final String solrxml = "<solr><cores adminHandler=\"/admin\"></cores></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals("<solr><cores adminHandler=\"/admin\"></cores></solr>", persistor.buildSolrXML(EMPTY_CD_LIST));
  }

  @Test
  public void emptySolrXmlIsPersisted() {
    final String solrxml = "<solr></solr>";

    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    assertEquals("<solr><cores></cores></solr>", persistor.buildSolrXML(EMPTY_CD_LIST));
  }

  @Test
  public void simpleCoreDescriptorIsPersisted() throws IOException {
    
    final String solrxml = "<solr><cores></cores></solr>";
    
    final File solrHomeDirectory = createTempDir();
    
    copyMinFullSetup(solrHomeDirectory);
    
    CoreContainer cc = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    
    final CoreDescriptor cd = new CoreDescriptor(cc, "testcore",
        "instance/dir/");
    List<CoreDescriptor> cds = ImmutableList.of(cd);
    
    SolrXMLCoresLocator persistor = new SolrXMLCoresLocator(solrxml, null);
    String xml = persistor.buildSolrXML(cds);
    
    assertTrue(xml.contains("<solr><cores>"));
    assertTrue(xml.contains("name=\"testcore\""));
    assertTrue(xml.contains("instanceDir=\"instance/dir/\""));
    assertTrue(xml.contains("</cores></solr>"));
  }

  @Test
  public void shardHandlerInfoIsPersisted() {

    final String solrxml =
        "<solr>" +
          "<cores adminHandler=\"whatever\">" +
            "<core name=\"testcore\" instanceDir=\"instance/dir/\"/>" +
            "<shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">" +
              "<int name=\"socketTimeout\">${socketTimeout:500}</int>" +
              "<str name=\"arbitrary\">arbitraryValue</str>" +
            "</shardHandlerFactory>" +
          "</cores>" +
        "</solr>";

    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(solrxml, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
    assertTrue(locator.getTemplate().contains("<shardHandlerFactory "));
    assertTrue(locator.getTemplate().contains("${socketTimeout:500}"));

  }

  @Test
  public void simpleShardHandlerInfoIsPersisted() {

    final String solrxml =
        "<solr>" +
          "<cores adminHandler=\"whatever\">" +
            "<core name=\"testcore\" instanceDir=\"instance/dir/\"/>" +
            "<shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\"/>" +
          "</cores>" +
        "</solr>";

    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(solrxml, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
    assertTrue(locator.getTemplate().contains("<shardHandlerFactory "));
  }

  @Test
  public void complexXmlIsParsed() {
    SolrXMLCoresLocator locator = new SolrXMLCoresLocator(TestSolrXmlPersistence.SOLR_XML_LOTS_SYSVARS, null);
    assertTrue(locator.getTemplate().contains("{{CORES_PLACEHOLDER}}"));
  }

}
