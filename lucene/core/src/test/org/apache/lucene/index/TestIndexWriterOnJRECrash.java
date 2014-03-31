package org.apache.lucene.index;

/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.SeedUtils;
/**
 * Runs TestNRTThreads in a separate process, crashes the JRE in the middle
 * of execution, then runs checkindex to make sure its not corrupt.
 */
public class TestIndexWriterOnJRECrash extends TestNRTThreads {
  private File tempDir;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    tempDir = TestUtil.getTempDir("jrecrash");
    tempDir.delete();
    tempDir.mkdir();
  }
  
  @Override @Nightly
  public void testNRTThreads() throws Exception {
    // if we are not the fork
    if (System.getProperty("tests.crashmode") == null) {
      // try up to 10 times to create an index
      for (int i = 0; i < 10; i++) {
        forkTest();
        // if we succeeded in finding an index, we are done.
        if (checkIndexes(tempDir))
          return;
      }
    } else {
      // note: re-enable this if we create a 4.x impersonator,
      // and if its format is actually different than the real 4.x (unlikely)
      // TODO: the non-fork code could simply enable impersonation?
      // assumeFalse("does not support PreFlex, see LUCENE-3992", 
      //    Codec.getDefault().getName().equals("Lucene4x"));
      
      // we are the fork, setup a crashing thread
      final int crashTime = TestUtil.nextInt(random(), 3000, 4000);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(crashTime);
          } catch (InterruptedException e) {}
          crashJRE();
        }
      };
      t.setPriority(Thread.MAX_PRIORITY);
      t.start();
      // run the test until we crash.
      for (int i = 0; i < 1000; i++) {
        super.testNRTThreads();
      }
    }
  }
  
  /** fork ourselves in a new jvm. sets -Dtests.crashmode=true */
  public void forkTest() throws Exception {
    List<String> cmd = new ArrayList<>();
    cmd.add(System.getProperty("java.home") 
        + System.getProperty("file.separator")
        + "bin"
        + System.getProperty("file.separator")
        + "java");
    cmd.add("-Xmx512m");
    cmd.add("-Dtests.crashmode=true");
    // passing NIGHTLY to this test makes it run for much longer, easier to catch it in the act...
    cmd.add("-Dtests.nightly=true");
    cmd.add("-DtempDir=" + tempDir.getPath());
    cmd.add("-Dtests.seed=" + SeedUtils.formatSeed(random().nextLong()));
    cmd.add("-ea");
    cmd.add("-cp");
    cmd.add(System.getProperty("java.class.path"));
    cmd.add("org.junit.runner.JUnitCore");
    cmd.add(getClass().getName());
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(tempDir);
    pb.redirectErrorStream(true);
    Process p = pb.start();

    // We pump everything to stderr.
    PrintStream childOut = System.err; 
    Thread stdoutPumper = ThreadPumper.start(p.getInputStream(), childOut);
    Thread stderrPumper = ThreadPumper.start(p.getErrorStream(), childOut);
    if (VERBOSE) childOut.println(">>> Begin subprocess output");
    p.waitFor();
    stdoutPumper.join();
    stderrPumper.join();
    if (VERBOSE) childOut.println("<<< End subprocess output");
  }

  /** A pipe thread. It'd be nice to reuse guava's implementation for this... */
  static class ThreadPumper {
    public static Thread start(final InputStream from, final OutputStream to) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            byte [] buffer = new byte [1024];
            int len;
            while ((len = from.read(buffer)) != -1) {
              if (VERBOSE) {
                to.write(buffer, 0, len);
              }
            }
          } catch (IOException e) {
            System.err.println("Couldn't pipe from the forked process: " + e.toString());
          }
        }
      };
      t.start();
      return t;
    }
  }
  
  /**
   * Recursively looks for indexes underneath <code>file</code>,
   * and runs checkindex on them. returns true if it found any indexes.
   */
  public boolean checkIndexes(File file) throws IOException {
    if (file.isDirectory()) {
      BaseDirectoryWrapper dir = newFSDirectory(file);
      dir.setCheckIndexOnClose(false); // don't double-checkindex
      if (DirectoryReader.indexExists(dir)) {
        if (VERBOSE) {
          System.err.println("Checking index: " + file);
        }
        // LUCENE-4738: if we crashed while writing first
        // commit it's possible index will be corrupt (by
        // design we don't try to be smart about this case
        // since that too risky):
        if (SegmentInfos.getLastCommitGeneration(dir) > 1) {
          TestUtil.checkIndex(dir);
        }
        dir.close();
        return true;
      }
      dir.close();
      for (File f : file.listFiles())
        if (checkIndexes(f))
          return true;
    }
    return false;
  }

  /**
   * currently, this only works/tested on Sun and IBM.
   */
  public void crashJRE() {
    final String vendor = Constants.JAVA_VENDOR;
    final boolean supportsUnsafeNpeDereference = 
        vendor.startsWith("Oracle") || 
        vendor.startsWith("Sun") || 
        vendor.startsWith("Apple");

      try {
        if (supportsUnsafeNpeDereference) {
          try {
            Class<?> clazz = Class.forName("sun.misc.Unsafe");
            Field field = clazz.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Object o = field.get(null);
            Method m = clazz.getMethod("putAddress", long.class, long.class);
            m.invoke(o, 0L, 0L);
          } catch (Throwable e) {
            System.out.println("Couldn't kill the JVM via Unsafe.");
            e.printStackTrace(System.out); 
          }
        }

        // Fallback attempt to Runtime.halt();
        Runtime.getRuntime().halt(-1);
      } catch (Exception e) {
        System.out.println("Couldn't kill the JVM.");
        e.printStackTrace(System.out); 
      }

      // We couldn't get the JVM to crash for some reason.
      fail();
  }
}
