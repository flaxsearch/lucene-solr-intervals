package org.apache.solr.store.hdfs;

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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread (HADOOP-9049)
//@Ignore("this test violates the test security policy because of org.apache.hadoop.fs.RawLocalFileSystem.mkdirs")
public class HdfsDirectoryTest extends SolrTestCaseJ4 {
  
  private static final int MAX_NUMBER_OF_WRITES = 10000;
  private static final int MIN_FILE_SIZE = 100;
  private static final int MAX_FILE_SIZE = 100000;
  private static final int MIN_BUFFER_SIZE = 1;
  private static final int MAX_BUFFER_SIZE = 5000;
  private static final int MAX_NUMBER_OF_READS = 10000;
  private static MiniDFSCluster dfsCluster;
  private HdfsDirectory directory;
  private Random random;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createTempDir();
    dfsCluster = HdfsTestUtil.setupClass(TEMP_DIR.getAbsolutePath()
        + File.separator + HdfsDirectoryTest.class.getName() + "_hdfsdir-"
        + System.currentTimeMillis());
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    
    Configuration conf = new Configuration();
    conf.set("dfs.permissions.enabled", "false");
    
    directory = new HdfsDirectory(new Path(dfsCluster.getURI().toString() + dataDir.getAbsolutePath() + "/hdfs"), conf);
    
    random = random();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testWritingAndReadingAFile() throws IOException {
    String[] listAll = directory.listAll();
    for (String file : listAll) {
      directory.deleteFile(file);
    }
    
    IndexOutput output = directory.createOutput("testing.test", new IOContext());
    output.writeInt(12345);
    output.flush();
    output.close();

    IndexInput input = directory.openInput("testing.test", new IOContext());
    assertEquals(12345, input.readInt());
    input.close();

    listAll = directory.listAll();
    assertEquals(1, listAll.length);
    assertEquals("testing.test", listAll[0]);

    assertEquals(4, directory.fileLength("testing.test"));

    IndexInput input1 = directory.openInput("testing.test", new IOContext());

    IndexInput input2 = (IndexInput) input1.clone();
    assertEquals(12345, input2.readInt());
    input2.close();

    assertEquals(12345, input1.readInt());
    input1.close();

    assertFalse(directory.fileExists("testing.test.other"));
    assertTrue(directory.fileExists("testing.test"));
    directory.deleteFile("testing.test");
    assertFalse(directory.fileExists("testing.test"));
  }
  
  @Test
  public void testEOF() throws IOException {
    Directory fsDir = new RAMDirectory();
    String name = "test.eof";
    createFile(name, fsDir, directory);
    long fsLength = fsDir.fileLength(name);
    long hdfsLength = directory.fileLength(name);
    assertEquals(fsLength, hdfsLength);
    testEof(name,fsDir,fsLength);
    testEof(name,directory,hdfsLength);
  }

  private void testEof(String name, Directory directory, long length) throws IOException {
    IndexInput input = directory.openInput(name, new IOContext());
    input.seek(length);
    try {
      input.readByte();
      fail("should throw eof");
    } catch (IOException e) {
    }
  }

  @Test
  public void testRandomAccessWrites() throws IOException {
    int i = 0;
    try {
      Set<String> names = new HashSet<String>();
      for (; i< 10; i++) {
        Directory fsDir = new RAMDirectory();
        String name = getName();
        System.out.println("Working on pass [" + i  +"] contains [" + names.contains(name) + "]");
        names.add(name);
        createFile(name,fsDir,directory);
        assertInputsEquals(name,fsDir,directory);
        fsDir.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed on pass [" + i + "]");
    }
  }

  private void assertInputsEquals(String name, Directory fsDir, HdfsDirectory hdfs) throws IOException {
    int reads = random.nextInt(MAX_NUMBER_OF_READS);
    IndexInput fsInput = fsDir.openInput(name,new IOContext());
    IndexInput hdfsInput = hdfs.openInput(name,new IOContext());
    assertEquals(fsInput.length(), hdfsInput.length());
    int fileLength = (int) fsInput.length();
    for (int i = 0; i < reads; i++) {
      int nextInt = Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE,fileLength);
      byte[] fsBuf = new byte[random.nextInt(nextInt > 0 ? nextInt : 1) + MIN_BUFFER_SIZE];
      byte[] hdfsBuf = new byte[fsBuf.length];
      int offset = random.nextInt(fsBuf.length);
      
      nextInt = fsBuf.length - offset;
      int length = random.nextInt(nextInt > 0 ? nextInt : 1);
      nextInt = fileLength - length;
      int pos = random.nextInt(nextInt > 0 ? nextInt : 1);
      fsInput.seek(pos);
      fsInput.readBytes(fsBuf, offset, length);
      hdfsInput.seek(pos);
      hdfsInput.readBytes(hdfsBuf, offset, length);
      for (int f = offset; f < length; f++) {
        if (fsBuf[f] != hdfsBuf[f]) {
          fail();
        }
      }
    }
    fsInput.close();
    hdfsInput.close();
  }

  private void createFile(String name, Directory fsDir, HdfsDirectory hdfs) throws IOException {
    int writes = random.nextInt(MAX_NUMBER_OF_WRITES);
    int fileLength = random.nextInt(MAX_FILE_SIZE - MIN_FILE_SIZE) + MIN_FILE_SIZE;
    IndexOutput fsOutput = fsDir.createOutput(name, new IOContext());
    fsOutput.setLength(fileLength);
    IndexOutput hdfsOutput = hdfs.createOutput(name, new IOContext());
    hdfsOutput.setLength(fileLength);
    for (int i = 0; i < writes; i++) {
      byte[] buf = new byte[random.nextInt(Math.min(MAX_BUFFER_SIZE - MIN_BUFFER_SIZE,fileLength)) + MIN_BUFFER_SIZE];
      random.nextBytes(buf);
      int offset = random.nextInt(buf.length);
      int length = random.nextInt(buf.length - offset);
      fsOutput.writeBytes(buf, offset, length);
      hdfsOutput.writeBytes(buf, offset, length);
    }
    fsOutput.close();
    hdfsOutput.close();
  }

  private String getName() {
    return Long.toString(Math.abs(random.nextLong()));
  }

}
