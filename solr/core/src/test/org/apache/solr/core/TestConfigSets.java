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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.internal.matchers.StringContains.containsString;

public class TestConfigSets extends SolrTestCaseJ4 {

  @Rule
  public TestRule testRule = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  public static String solrxml = "<solr><str name=\"configSetBaseDir\">${configsets:configsets}</str></solr>";

  public CoreContainer setupContainer(String testName, String configSetsBaseDir) {

    File testDirectory = new File(TEMP_DIR, testName);
    testDirectory.mkdirs();

    System.setProperty("configsets", configSetsBaseDir);

    SolrResourceLoader loader = new SolrResourceLoader(testDirectory.getAbsolutePath());
    CoreContainer container = new CoreContainer(loader, ConfigSolr.fromString(loader, solrxml));
    container.load();

    return container;
  }

  @Test
  public void testConfigSetServiceFindsConfigSets() {
    CoreContainer container = null;
    try {
      container = setupContainer("findsConfigSets", getFile("solr/configsets").getAbsolutePath());
      String testDirectory = container.getResourceLoader().getInstanceDir();

      SolrCore core1 = container.create("core1", testDirectory + "/core1", "configSet", "configset-2");
      assertThat(core1.getCoreDescriptor().getName(), is("core1"));
      assertThat(core1.getDataDir(), is(testDirectory + "/core1" + File.separator + "data" + File.separator));
      core1.close();

    }
    finally {
      if (container != null)
        container.shutdown();
    }
  }

  @Test
  public void testNonExistentConfigSetThrowsException() {
    CoreContainer container = null;
    try {
      container = setupContainer("badConfigSet", getFile("solr/configsets").getAbsolutePath());
      String testDirectory = container.getResourceLoader().getInstanceDir();

      container.create("core1", testDirectory + "/core1", "configSet", "nonexistent");
      fail("Expected core creation to fail");
    }
    catch (Exception e) {
      Throwable wrappedException = getWrappedException(e);
      assertThat(wrappedException.getMessage(), containsString("nonexistent"));
    }
    finally {
      if (container != null)
        container.shutdown();
    }
  }

  @Test
  public void testConfigSetOnCoreReload() throws IOException {
    File testDirectory = new File(TEMP_DIR, "core-reload");
    testDirectory.mkdirs();
    File configSetsDir = new File(testDirectory, "configsets");

    FileUtils.copyDirectory(getFile("solr/configsets"), configSetsDir);

    String csd = configSetsDir.getAbsolutePath();
    System.setProperty("configsets", csd);

    SolrResourceLoader loader = new SolrResourceLoader(testDirectory.getAbsolutePath());
    CoreContainer container = new CoreContainer(loader, ConfigSolr.fromString(loader, solrxml));
    container.load();

    // We initially don't have a /get handler defined
    SolrCore core = container.create("core1", testDirectory + "/core", "configSet", "configset-2");
    container.register(core, false);
    assertThat("No /get handler should be defined in the initial configuration",
        core.getRequestHandler("/get"), is(nullValue()));

    // Now copy in a config with a /get handler and reload
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-withgethandler.xml"),
        new File(new File(configSetsDir, "configset-2/conf"), "solrconfig.xml"));
    container.reload("core1");

    core = container.getCore("core1");
    assertThat("A /get handler should be defined in the reloaded configuration",
        core.getRequestHandler("/get"), is(notNullValue()));
    core.close();

    container.shutdown();
  }

}
