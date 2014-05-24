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
import java.util.Map;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.Test;

public class SolrCoreCheckLockOnStartupTest extends SolrTestCaseJ4 {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty("solr.directoryFactory", "org.apache.solr.core.SimpleFSDirectoryFactory");
    // test tests native and simple in the same jvm in the same exact directory:
    // the file will remain after the native test (it cannot safely be deleted without the risk of deleting another guys lock)
    // its ok, these aren't "compatible" anyway: really this test should not re-use the same directory at all.
    new File(new File(initCoreDataDir, "index"), IndexWriter.WRITE_LOCK_NAME).delete();
  }

  @Test
  public void testSimpleLockErrorOnStartup() throws Exception {

    Directory directory = newFSDirectory(new File(initCoreDataDir, "index"), new SimpleFSLockFactory());
    //creates a new IndexWriter without releasing the lock yet
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(TEST_VERSION_CURRENT, null));

    ignoreException("locked");
    try {
      System.setProperty("solr.tests.lockType","simple");
      //opening a new core on the same index
      initCore("solrconfig-basic.xml", "schema.xml");
      if (checkForCoreInitException(LockObtainFailedException.class))
        return;
      fail("Expected " + LockObtainFailedException.class.getSimpleName());
    } finally {
      System.clearProperty("solr.tests.lockType");
      unIgnoreException("locked");
      indexWriter.shutdown();
      directory.close();
      deleteCore();
    }
  }

  @Test
  public void testNativeLockErrorOnStartup() throws Exception {

    File indexDir = new File(initCoreDataDir, "index");
    log.info("Acquiring lock on {}", indexDir.getAbsolutePath());
    Directory directory = newFSDirectory(indexDir, new NativeFSLockFactory());
    //creates a new IndexWriter without releasing the lock yet
    IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(TEST_VERSION_CURRENT, null));

    ignoreException("locked");
    try {
      System.setProperty("solr.tests.lockType","native");
      //opening a new core on the same index
      initCore("solrconfig-basic.xml", "schema.xml");
      CoreContainer cc = h.getCoreContainer();
      if (checkForCoreInitException(LockObtainFailedException.class))
        return;
      fail("Expected " + LockObtainFailedException.class.getSimpleName());
    } finally {
      System.clearProperty("solr.tests.lockType");
      unIgnoreException("locked");
      indexWriter.shutdown();
      directory.close();
      deleteCore();
    }
  }

  private boolean checkForCoreInitException(Class<? extends Exception> clazz) {
    for (Map.Entry<String, Exception> entry : h.getCoreContainer().getCoreInitFailures().entrySet()) {
      for (Throwable t = entry.getValue(); t != null; t = t.getCause()) {
        if (clazz.isInstance(t))
          return true;
      }
    }
    return false;
  }
}
