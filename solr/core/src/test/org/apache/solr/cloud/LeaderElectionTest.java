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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
public class LeaderElectionTest extends SolrTestCaseJ4 {
  
  static final int TIMEOUT = 30000;
  private ZkTestServer server;
  private SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private Map<Integer,Thread> seqToThread;
  
  private volatile boolean stopStress = false;
  
  @BeforeClass
  public static void beforeClass() {
    createTempDir();
  }
  
  @AfterClass
  public static void afterClass() {

  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    server = new ZkTestServer(zkDir);
    server.setTheTickTime(1000);
    server.run();
    AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
    AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
    zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
    zkStateReader = new ZkStateReader(zkClient);
    seqToThread = Collections.synchronizedMap(new HashMap<Integer,Thread>());
  }
  
  class ClientThread extends Thread {
    SolrZkClient zkClient;
    private int nodeNumber;
    private volatile int seq = -1;
    private volatile boolean stop;
    private volatile boolean electionDone = false;
    private final ZkNodeProps props;
    
    public ClientThread(int nodeNumber) throws Exception {
      super("Thread-" + nodeNumber);
      
      props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP, Integer.toString(nodeNumber), ZkStateReader.CORE_NAME_PROP, "");

      this.zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT, TIMEOUT, new OnReconnect() {
        
        @Override
        public void command() {
          try {
            setupOnConnect();
          } catch (Throwable t) {
          } 
        }
      });
     this.nodeNumber = nodeNumber;
    }
    
    private void setupOnConnect() throws InterruptedException, KeeperException,
        IOException {
      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      LeaderElector elector = new LeaderElector(zkClient);
      ShardLeaderElectionContextBase context = new ShardLeaderElectionContextBase(
          elector, "shard1", "collection1", Integer.toString(nodeNumber),
          props, zkStateReader);
      elector.setup(context);
      seq = elector.joinElection(context, false);
      electionDone = true;
      seqToThread.put(seq, this);
    }
    
    @Override
    public void run() {
      try {
        setupOnConnect();
      } catch (InterruptedException e) {
        log.error("setup failed", e);
        
        if (this.zkClient != null) {
          this.zkClient.close();
        }

        return;
      } catch (Throwable e) {
        log.error("setup failed", e);
        
        if (this.zkClient != null) {
          this.zkClient.close();
        }
        
        return;
      }
        
      while (!stop) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }
      
    }
    
    public void close() throws InterruptedException {
      if (!zkClient.isClosed()) {
        zkClient.close();
      }
      this.stop = true;
    }

    public int getSeq() {
      return seq;
    }

    public int getNodeNumber() {
      return nodeNumber;
    }
  }

  @Test
  public void testBasic() throws Exception {
    LeaderElector elector = new LeaderElector(zkClient);
    ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "");
    ElectionContext context = new ShardLeaderElectionContextBase(elector,
        "shard2", "collection1", "dummynode1", props, zkStateReader);
    elector.setup(context);
    elector.joinElection(context, false);
    assertEquals("http://127.0.0.1/solr/",
        getLeaderUrl("collection1", "shard2"));
  }

  @Test
  public void testCancelElection() throws Exception {
    LeaderElector first = new LeaderElector(zkClient);
    ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "1");
    ElectionContext firstContext = new ShardLeaderElectionContextBase(first,
        "slice1", "collection2", "dummynode1", props, zkStateReader);
    first.setup(firstContext);
    first.joinElection(firstContext, false);

    Thread.sleep(1000);
    assertEquals("original leader was not registered", "http://127.0.0.1/solr/1/", getLeaderUrl("collection2", "slice1"));

    LeaderElector second = new LeaderElector(zkClient);
    props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
        "http://127.0.0.1/solr/", ZkStateReader.CORE_NAME_PROP, "2");
    ElectionContext context = new ShardLeaderElectionContextBase(second,
        "slice1", "collection2", "dummynode1", props, zkStateReader);
    second.setup(context);
    second.joinElection(context, false);
    Thread.sleep(1000);
    assertEquals("original leader should have stayed leader", "http://127.0.0.1/solr/1/", getLeaderUrl("collection2", "slice1"));
    firstContext.cancelElection();
    Thread.sleep(1000);
    assertEquals("new leader was not registered", "http://127.0.0.1/solr/2/", getLeaderUrl("collection2", "slice1"));
  }

  private String getLeaderUrl(final String collection, final String slice)
      throws KeeperException, InterruptedException {
    int iterCount = 60;
    while (iterCount-- > 0) {
      try {
        byte[] data = zkClient.getData(
            ZkStateReader.getShardLeadersPath(collection, slice), null, null,
            true);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(
            ZkNodeProps.load(data));
        return leaderProps.getCoreUrl();
      } catch (NoNodeException e) {
        Thread.sleep(500);
      }
    }
    zkClient.printLayoutToStdOut();
    throw new RuntimeException("Could not get leader props");
  }

  @Test
  public void testElection() throws Exception {
    
    List<ClientThread> threads = new ArrayList<ClientThread>();
    
    for (int i = 0; i < 15; i++) {
      ClientThread thread = new ClientThread(i);
      threads.add(thread);
    }
    try {
      for (Thread thread : threads) {
        thread.start();
      }
      
      while (true) { // wait for election to complete
        int doneCount = 0;
        for (ClientThread thread : threads) {
          if (thread.electionDone) {
            doneCount++;
          }
        }
        if (doneCount == 15) {
          break;
        }
        Thread.sleep(100);
      }
      
      int leaderThread = getLeaderThread();
      
      // whoever the leader is, should be the n_0 seq
      assertEquals(0, threads.get(leaderThread).seq);
      
      // kill n_0, 1, 3 and 4
      ((ClientThread) seqToThread.get(0)).close();
      
      waitForLeader(threads, 1);
      
      leaderThread = getLeaderThread();
      
      // whoever the leader is, should be the n_1 seq
      
      assertEquals(1, threads.get(leaderThread).seq);
      
      ((ClientThread) seqToThread.get(4)).close();
      ((ClientThread) seqToThread.get(1)).close();
      ((ClientThread) seqToThread.get(3)).close();
      
      // whoever the leader is, should be the n_2 seq
      
      waitForLeader(threads, 2);
      
      leaderThread = getLeaderThread();
      assertEquals(2, threads.get(leaderThread).seq);
      
      // kill n_5, 2, 6, 7, and 8
      ((ClientThread) seqToThread.get(5)).close();
      ((ClientThread) seqToThread.get(2)).close();
      ((ClientThread) seqToThread.get(6)).close();
      ((ClientThread) seqToThread.get(7)).close();
      ((ClientThread) seqToThread.get(8)).close();
      
      waitForLeader(threads, 9);
      leaderThread = getLeaderThread();
      
      // whoever the leader is, should be the n_9 seq
      assertEquals(9, threads.get(leaderThread).seq);
      
    } finally {
      // cleanup any threads still running
      for (ClientThread thread : threads) {
        thread.close();
        thread.interrupt();
        
      }
      
      for (Thread thread : threads) {
        thread.join();
      }
    }
    
  }

  private void waitForLeader(List<ClientThread> threads, int seq)
      throws KeeperException, InterruptedException {
    int leaderThread;
    int tries = 0;
    leaderThread = getLeaderThread();
    while (threads.get(leaderThread).seq < seq) {
      leaderThread = getLeaderThread();
      if (tries++ > 50) {
        break;
      }
      Thread.sleep(200);
    }
  }

  private int getLeaderThread() throws KeeperException, InterruptedException {
    String leaderUrl = getLeaderUrl("collection1", "shard1");
    return Integer.parseInt(leaderUrl.replaceAll("/", ""));
  }
  
  @Test
  public void testStressElection() throws Exception {
    final ScheduledExecutorService scheduler = Executors
        .newScheduledThreadPool(15, new DefaultSolrThreadFactory("stressElection"));
    final List<ClientThread> threads = Collections
        .synchronizedList(new ArrayList<ClientThread>());
    
    // start with a leader
    ClientThread thread1 = null;
    thread1 = new ClientThread(0);
    threads.add(thread1);
    scheduler.schedule(thread1, 0, TimeUnit.MILLISECONDS);
    
    Thread.sleep(2000);

    Thread scheduleThread = new Thread() {
      @Override
      public void run() {
        int count = atLeast(5);
        for (int i = 1; i < count; i++) {
          int launchIn = random().nextInt(500);
          ClientThread thread = null;
          try {
            thread = new ClientThread(i);
          } catch (Exception e) {
            //
          }
          if (thread != null) {
            threads.add(thread);
            scheduler.schedule(thread, launchIn, TimeUnit.MILLISECONDS);
          }
        }
      }
    };
    
    Thread killThread = new Thread() {
      @Override
      public void run() {
        
        while (!stopStress) {
          try {
            int j;
            try {
              // always 1 we won't kill...
              j = random().nextInt(threads.size() - 2);
            } catch(IllegalArgumentException e) {
              continue;
            }
            try {
              threads.get(j).close();
            } catch (InterruptedException e) {
              throw e;
            } catch (Exception e) {
              
            }

            Thread.sleep(10);
          } catch (Exception e) {
          }
        }
      }
    };
    
    Thread connLossThread = new Thread() {
      @Override
      public void run() {
        
        while (!stopStress) {
          try {
            Thread.sleep(50);
            int j;
            j = random().nextInt(threads.size());
            try {
              threads.get(j).zkClient.getSolrZooKeeper().pauseCnxn(
                  ZkTestServer.TICK_TIME * 2);
            } catch (Exception e) {
              e.printStackTrace();
            }
            Thread.sleep(500);
            
          } catch (Exception e) {
            
          }
        }
      }
    };
    
    scheduleThread.start();
    connLossThread.start();
    killThread.start();
    
    Thread.sleep(4000);
    
    stopStress = true;
    
    scheduleThread.interrupt();
    connLossThread.interrupt();
    killThread.interrupt();
    
    scheduleThread.join();
    scheduler.shutdownNow();
    
    connLossThread.join();
    killThread.join();
    
    int seq = threads.get(getLeaderThread()).getSeq();
    
    // we have a leader we know, TODO: lets check some other things
    
    // cleanup any threads still running
    for (ClientThread thread : threads) {
      thread.zkClient.getSolrZooKeeper().close();
      thread.close();
    }
    
    for (Thread thread : threads) {
      thread.join();
    }

    
  }
  
  @Override
  public void tearDown() throws Exception {
    zkClient.close();
    zkStateReader.close();
    server.shutdown();
    super.tearDown();
  }
  
  private void printLayout(String zkHost) throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkHost, AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
