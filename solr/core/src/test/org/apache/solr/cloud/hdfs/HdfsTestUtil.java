package org.apache.solr.cloud.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;

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

public class HdfsTestUtil {
  
  private static Locale savedLocale;
  
  private static Map<MiniDFSCluster,Timer> timers = new ConcurrentHashMap<>();

  public static MiniDFSCluster setupClass(String dataDir) throws Exception {
    LuceneTestCase.assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
      Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));
    File dir = new File(dataDir);
    new File(dataDir).mkdirs();

    savedLocale = Locale.getDefault();
    // TODO: we HACK around HADOOP-9643
    Locale.setDefault(Locale.ENGLISH);
    
    int dataNodes = 2;
    
    Configuration conf = new Configuration();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions.enabled", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hdfs.minidfs.basedir", dir.getAbsolutePath() + File.separator + "hdfsBaseDir");
    conf.set("dfs.namenode.name.dir", dir.getAbsolutePath() + File.separator + "nameNodeNameDir");
    
    
    System.setProperty("test.build.data", dir.getAbsolutePath() + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", dir.getAbsolutePath() + File.separator + "hdfs" + File.separator + "cache");
    System.setProperty("solr.lock.type", "hdfs");
    
    System.setProperty("solr.hdfs.home", "/solr_hdfs_home");
    
    System.setProperty("solr.hdfs.blockcache.global", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    
    final MiniDFSCluster dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
    dfsCluster.waitActive();
    
    NameNodeAdapter.enterSafeMode(dfsCluster.getNameNode(), false);
    
    int rnd = LuceneTestCase.random().nextInt(10000);
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      
      @Override
      public void run() {
        NameNodeAdapter.leaveSafeMode(dfsCluster.getNameNode());
      }
    }, rnd);
    
    timers.put(dfsCluster, timer);
    
    SolrTestCaseJ4.useFactory("org.apache.solr.core.HdfsDirectoryFactory");
    
    return dfsCluster;
  }
  
  public static void teardownClass(MiniDFSCluster dfsCluster) throws Exception {
    SolrTestCaseJ4.resetFactory();
    System.clearProperty("solr.lock.type");
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.hdfs.blockcache.global");
    if (dfsCluster != null) {
      timers.remove(dfsCluster);
      dfsCluster.shutdown();
    }
    
    // TODO: we HACK around HADOOP-9643
    if (savedLocale != null) {
      Locale.setDefault(savedLocale);
    }
  }
  
  public static String getDataDir(MiniDFSCluster dfsCluster, String dataDir)
      throws IOException {
    if (dataDir == null) {
      return null;
    }
    URI uri = dfsCluster.getURI();
    String dir = uri.toString()
        + "/"
        + new File(dataDir).toString().replaceAll(":", "_")
            .replaceAll("/", "_");
    return dir;
  }

}
