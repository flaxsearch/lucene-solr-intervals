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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ZkSolrClientTest extends AbstractSolrTestCase {
  private static final boolean DEBUG = false;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  public void testConnect() throws Exception {
    String zkDir = createTempDir("zkData").getAbsolutePath();
    ZkTestServer server = null;

    server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);

    zkClient.close();
    server.shutdown();
  }

  public void testMakeRootNode() throws Exception {
    String zkDir = createTempDir("zkData").getAbsolutePath();
    ZkTestServer server = null;

    server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

    SolrZkClient zkClient = new SolrZkClient(server.getZkHost(),
        AbstractZkTestCase.TIMEOUT);

    assertTrue(zkClient.exists("/solr", true));

    zkClient.close();
    server.shutdown();
  }
  
  public void testClean() throws Exception {
    String zkDir = createTempDir("zkData").getAbsolutePath();
    ZkTestServer server = null;

    server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    

    SolrZkClient zkClient = new SolrZkClient(server.getZkHost(),
        AbstractZkTestCase.TIMEOUT);

    zkClient.makePath("/test/path/here", true);
    
    zkClient.makePath("/zz/path/here", true);
    
    zkClient.clean("/");
    
    assertFalse(zkClient.exists("/test", true));
    assertFalse(zkClient.exists("/zz", true));

    zkClient.close();
    server.shutdown();
  }

  public void testReconnect() throws Exception {
    String zkDir = createTempDir("zkData").getAbsolutePath();
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath, false, true);

      zkClient.makePath("collections/collection1", false, true);
      int zkServerPort = server.getPort();
      // this tests disconnect state
      server.shutdown();

      Thread.sleep(80);


      try {
        zkClient.makePath("collections/collection2", false);
        Assert.fail("Server should be down here");
      } catch (KeeperException.ConnectionLossException e) {

      }

      // bring server back up
      server = new ZkTestServer(zkDir, zkServerPort);
      server.run();

      // TODO: can we do better?
      // wait for reconnect
      Thread.sleep(600);

      try {
        zkClient.makePath("collections/collection3", true);
      } catch (KeeperException.ConnectionLossException e) {
        Thread.sleep(5000); // try again in a bit
        zkClient.makePath("collections/collection3", true);
      }

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }

      assertNotNull(zkClient.exists("/collections/collection3", null, true));
      assertNotNull(zkClient.exists("/collections/collection1", null, true));
      
      // simulate session expiration
      
      // one option
      long sessionId = zkClient.getSolrZooKeeper().getSessionId();
      server.expire(sessionId);
      
      // another option
      //zkClient.getSolrZooKeeper().getConnection().disconnect();

      // this tests expired state

      Thread.sleep(1000); // pause for reconnect
      
      for (int i = 0; i < 8; i++) {
        try {
          zkClient.makePath("collections/collection4", true);
          break;
        } catch (KeeperException.SessionExpiredException e) {

        } catch (KeeperException.ConnectionLossException e) {

        }
        Thread.sleep(1000 * i);
      }

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }

      assertNotNull("Node does not exist, but it should", zkClient.exists("/collections/collection4", null, true));

    } finally {

      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }

  public void testWatchChildren() throws Exception {
    String zkDir = createTempDir("zkData").getAbsolutePath();
    
    final AtomicInteger cnt = new AtomicInteger();
    ZkTestServer server = new ZkTestServer(zkDir);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    Thread.sleep(400);
    AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
    final SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
    try {
      final CountDownLatch latch = new CountDownLatch(1);
      zkClient.makePath("/collections", true);

      zkClient.getChildren("/collections", new Watcher() {

        @Override
        public void process(WatchedEvent event) {
          if (DEBUG) {
            System.out.println("children changed");
          }
          cnt.incrementAndGet();
          // remake watch
          try {
            zkClient.getChildren("/collections", this, true);
            latch.countDown();
          } catch (KeeperException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }, true);

      zkClient.makePath("/collections/collection99/shards", true);
      latch.await(); //wait until watch has been re-created

      zkClient.makePath("collections/collection99/config=collection1", true);

      zkClient.makePath("collections/collection99/config=collection3", true);
      
      zkClient.makePath("/collections/collection97/shards", true);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      
      // pause for the watches to fire
      Thread.sleep(700);
      
      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }
      
      if (cnt.intValue() < 2) {
        Thread.sleep(4000); // wait a bit more
      }
      
      assertEquals(2, cnt.intValue());

    } finally {

      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @AfterClass
  public static void afterClass() throws InterruptedException {
    // wait just a bit for any zk client threads to outlast timeout
    Thread.sleep(2000);
  }
}
