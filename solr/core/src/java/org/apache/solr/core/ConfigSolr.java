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

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public abstract class ConfigSolr {
  protected static Logger log = LoggerFactory.getLogger(ConfigSolr.class);
  
  public final static String SOLR_XML_FILE = "solr.xml";

  public static ConfigSolr fromFile(SolrResourceLoader loader, File configFile) {
    log.info("Loading container configuration from {}", configFile.getAbsolutePath());

    InputStream inputStream = null;

    try {
      if (!configFile.exists()) {
        log.info("{} does not exist, using default configuration", configFile.getAbsolutePath());
        inputStream = new ByteArrayInputStream(ConfigSolrXmlOld.DEF_SOLR_XML.getBytes(Charsets.UTF_8));
      }
      else {
        inputStream = new FileInputStream(configFile);
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ByteStreams.copy(inputStream, baos);
      String originalXml = IOUtils.toString(new ByteArrayInputStream(baos.toByteArray()), "UTF-8");
      return fromInputStream(loader, new ByteArrayInputStream(baos.toByteArray()), configFile, originalXml);
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load SOLR configuration", e);
    }
    finally {
      IOUtils.closeQuietly(inputStream);
    }
  }

  public static ConfigSolr fromString(String xml) {
    return fromInputStream(null, new ByteArrayInputStream(xml.getBytes(Charsets.UTF_8)), null, xml);
  }

  public static ConfigSolr fromInputStream(SolrResourceLoader loader, InputStream is, File file, String originalXml) {
    try {
      Config config = new Config(loader, null, new InputSource(is), null, false);
      return fromConfig(config, file, originalXml);
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static ConfigSolr fromSolrHome(SolrResourceLoader loader, String solrHome) {
    return fromFile(loader, new File(solrHome, SOLR_XML_FILE));
  }

  public static ConfigSolr fromConfig(Config config, File file, String originalXml) {
    boolean oldStyle = (config.getNode("solr/cores", false) != null);
    return oldStyle ? new ConfigSolrXmlOld(config, file, originalXml)
                    : new ConfigSolrXml(config);
  }
  
  public abstract CoresLocator getCoresLocator();


  public PluginInfo getShardHandlerFactoryPluginInfo() {
    Node node = config.getNode(getShardHandlerFactoryConfigPath(), false);
    return (node == null) ? null : new PluginInfo(node, "shardHandlerFactory", false, true);
  }

  public Node getUnsubsititutedShardHandlerFactoryPluginNode() {
    return config.getUnsubstitutedNode(getShardHandlerFactoryConfigPath(), false);
  }

  protected abstract String getShardHandlerFactoryConfigPath();

  // Ugly for now, but we'll at least be able to centralize all of the differences between 4x and 5x.
  public static enum CfgProp {
    SOLR_ADMINHANDLER,
    SOLR_CORELOADTHREADS,
    SOLR_COREROOTDIRECTORY,
    SOLR_DISTRIBUPDATECONNTIMEOUT,
    SOLR_DISTRIBUPDATESOTIMEOUT,
    SOLR_HOST,
    SOLR_HOSTCONTEXT,
    SOLR_HOSTPORT,
    SOLR_LEADERVOTEWAIT,
    SOLR_LOGGING_CLASS,
    SOLR_LOGGING_ENABLED,
    SOLR_LOGGING_WATCHER_SIZE,
    SOLR_LOGGING_WATCHER_THRESHOLD,
    SOLR_MANAGEMENTPATH,
    SOLR_SHAREDLIB,
    SOLR_SHARESCHEMA,
    SOLR_TRANSIENTCACHESIZE,
    SOLR_GENERICCORENODENAMES,
    SOLR_ZKCLIENTTIMEOUT,
    SOLR_ZKHOST,

    //TODO: Remove all of these elements for 5.0
    SOLR_PERSISTENT,
    SOLR_CORES_DEFAULT_CORE_NAME,
    SOLR_ADMINPATH
  }

  protected Config config;
  protected Map<CfgProp, String> propMap = new HashMap<CfgProp, String>();

  public ConfigSolr(Config config) {
    this.config = config;

  }

  // for extension & testing.
  protected ConfigSolr() {

  }
  
  public Config getConfig() {
    return config;
  }

  public int getInt(CfgProp prop, int def) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? def : Integer.parseInt(val);
  }

  public boolean getBool(CfgProp prop, boolean defValue) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? defValue : Boolean.parseBoolean(val);
  }

  public String get(CfgProp prop, String def) {
    String val = propMap.get(prop);
    if (val != null) val = PropertiesUtil.substituteProperty(val, null);
    return (val == null) ? def : val;
  }

  public Properties getSolrProperties(String path) {
    try {
      return readProperties(((NodeList) config.evaluate(
          path, XPathConstants.NODESET)).item(0));
    } catch (Throwable e) {
      SolrException.log(log, null, e);
    }
    return null;

  }
  
  protected Properties readProperties(Node node) throws XPathExpressionException {
    XPath xpath = config.getXPath();
    NodeList props = (NodeList) xpath.evaluate("property", node, XPathConstants.NODESET);
    Properties properties = new Properties();
    for (int i = 0; i < props.getLength(); i++) {
      Node prop = props.item(i);
      properties.setProperty(DOMUtil.getAttr(prop, "name"),
          PropertiesUtil.substituteProperty(DOMUtil.getAttr(prop, "value"), null));
    }
    return properties;
  }

}

