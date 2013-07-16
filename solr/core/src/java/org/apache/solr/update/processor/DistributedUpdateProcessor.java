package org.apache.solr.update.processor;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.SolrCmdDistributor.Node;
import org.apache.solr.update.SolrCmdDistributor.Response;
import org.apache.solr.update.SolrCmdDistributor.StdNode;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionBucket;
import org.apache.solr.update.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

// NOT mt-safe... create a new processor for each add thread
// TODO: we really should not wait for distrib after local? unless a certain replication factor is asked for
public class DistributedUpdateProcessor extends UpdateRequestProcessor {
  private static final String TEST_DISTRIB_SKIP_SERVERS = "test.distrib.skip.servers";
  public final static Logger log = LoggerFactory.getLogger(DistributedUpdateProcessor.class);

  /**
   * Values this processor supports for the <code>DISTRIB_UPDATE_PARAM</code>.
   * This is an implementation detail exposed solely for tests.
   * 
   * @see DistributingUpdateProcessorFactory#DISTRIB_UPDATE_PARAM
   */
  public static enum DistribPhase {
    NONE, TOLEADER, FROMLEADER;

    public static DistribPhase parseParam(final String param) {
      if (param == null || param.trim().isEmpty()) {
        return NONE;
      }
      try {
        return valueOf(param);
      } catch (IllegalArgumentException e) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST, "Illegal value for " + 
           DISTRIB_UPDATE_PARAM + ": " + param, e);
      }
    }
  }

  public static final String COMMIT_END_POINT = "commit_end_point";
  public static final String LOG_REPLAY = "log_replay";
  
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final UpdateRequestProcessor next;

  public static final String VERSION_FIELD = "_version_";

  private final UpdateHandler updateHandler;
  private final UpdateLog ulog;
  private final VersionInfo vinfo;
  private final boolean versionsStored;
  private boolean returnVersions = true; // todo: default to false and make configurable

  private NamedList addsResponse = null;
  private NamedList deleteResponse = null;
  private NamedList deleteByQueryResponse = null;
  private CharsRef scratch;
  
  private final SchemaField idField;
  
  private SolrCmdDistributor cmdDistrib;

  private boolean zkEnabled = false;

  private CloudDescriptor cloudDesc;
  private String collection;
  private ZkController zkController;
  
  // these are setup at the start of each request processing
  // method in this update processor
  private boolean isLeader = true;
  private boolean forwardToLeader = false;
  private boolean isSubShardLeader = false;
  private List<Node> nodes;

  private int numNodes;

  private UpdateCommand updateCommand;  // the current command this processor is working on.

  
  public DistributedUpdateProcessor(SolrQueryRequest req,
      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(next);
    this.rsp = rsp;
    this.next = next;
    this.idField = req.getSchema().getUniqueKeyField();
    // version init

    this.updateHandler = req.getCore().getUpdateHandler();
    this.ulog = updateHandler.getUpdateLog();
    this.vinfo = ulog == null ? null : ulog.getVersionInfo();
    versionsStored = this.vinfo != null && this.vinfo.getVersionField() != null;
    returnVersions = req.getParams().getBool(UpdateParams.VERSIONS ,false);

    // TODO: better way to get the response, or pass back info to it?
    SolrRequestInfo reqInfo = returnVersions ? SolrRequestInfo.getRequestInfo() : null;

    this.req = req;
    
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    
    this.zkEnabled  = coreDesc.getCoreContainer().isZooKeeperAware();
    zkController = req.getCore().getCoreDescriptor().getCoreContainer().getZkController();
    if (zkEnabled) {
      numNodes =  zkController.getZkStateReader().getClusterState().getLiveNodes().size();
      cmdDistrib = new SolrCmdDistributor(numNodes, coreDesc.getCoreContainer().getZkController().getUpdateShardHandler());
    }
    //this.rsp = reqInfo != null ? reqInfo.getRsp() : null;

    cloudDesc = coreDesc.getCloudDescriptor();
    
    if (cloudDesc != null) {
      collection = cloudDesc.getCollectionName();
    }
  }


  private List<Node> setupRequest(String id, SolrInputDocument doc) {
    List<Node> nodes = null;

    // if we are in zk mode...
    if (zkEnabled) {

      if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
        isLeader = false;     // we actually might be the leader, but we don't want leader-logic for these types of updates anyway.
        forwardToLeader = false;
        return nodes;
      }

      String coreName = req.getCore().getName();

      ClusterState cstate = zkController.getClusterState();
      numNodes = cstate.getLiveNodes().size();
      DocCollection coll = cstate.getCollection(collection);
      Slice slice = coll.getRouter().getTargetSlice(id, doc, req.getParams(), coll);

      if (slice == null) {
        // No slice found.  Most strict routers will have already thrown an exception, so a null return is
        // a signal to use the slice of this core.
        // TODO: what if this core is not in the targeted collection?
        String shardId = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
        slice = coll.getSlice(shardId);
        if (slice == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "No shard " + shardId + " in " + coll);
        }
      }


      String shardId = slice.getName();

      try {
        // Not equivalent to getLeaderProps, which does retries to find a leader.
        // Replica leader = slice.getLeader();
        Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
            collection, shardId);
        isLeader = leaderReplica.getName().equals(
            req.getCore().getCoreDescriptor().getCloudDescriptor()
                .getCoreNodeName());

        if (!isLeader) {
          isSubShardLeader = amISubShardLeader(coll, slice, id, doc);
          if (isSubShardLeader) {
            String myShardId = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
            slice = coll.getSlice(myShardId);
            shardId = myShardId;
            leaderReplica = zkController.getZkStateReader().getLeaderRetry(collection, myShardId);
            List<ZkCoreNodeProps> myReplicas = zkController.getZkStateReader().getReplicaProps(collection, shardId, leaderReplica.getName(), coreName, null, ZkStateReader.DOWN);
          }
        }

        DistribPhase phase =
            DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

        doDefensiveChecks(phase);


        if (DistribPhase.FROMLEADER == phase && !isSubShardLeader) {
          // we are coming from the leader, just go local - add no urls
          forwardToLeader = false;
        } else if (isLeader || isSubShardLeader) {
          // that means I want to forward onto my replicas...
          // so get the replicas...
          forwardToLeader = false;
          List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
              .getReplicaProps(collection, shardId, leaderReplica.getName(),
                  coreName, null, ZkStateReader.DOWN);

          if (replicaProps != null) {
            if (nodes == null)  {
            nodes = new ArrayList<Node>(replicaProps.size());
            }
            // check for test param that lets us miss replicas
            String[] skipList = req.getParams().getParams(TEST_DISTRIB_SKIP_SERVERS);
            Set<String> skipListSet = null;
            if (skipList != null) {
              skipListSet = new HashSet<String>(skipList.length);
              skipListSet.addAll(Arrays.asList(skipList));
              log.info("test.distrib.skip.servers was found and contains:" + skipListSet);
            }

            for (ZkCoreNodeProps props : replicaProps) {
              if (skipList != null) {
                boolean skip = skipListSet.contains(props.getCoreUrl());
                log.info("check url:" + props.getCoreUrl() + " against:" + skipListSet + " result:" + skip);
                if (!skip) {
                  nodes.add(new StdNode(props));
                }
              } else {
                nodes.add(new StdNode(props));
              }
            }
          }

        } else {
          // I need to forward onto the leader...
          nodes = new ArrayList<Node>(1);
          nodes.add(new RetryNode(new ZkCoreNodeProps(leaderReplica), zkController.getZkStateReader(), collection, shardId));
          forwardToLeader = true;
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }

    return nodes;
  }

  private boolean amISubShardLeader(DocCollection coll, Slice parentSlice, String id, SolrInputDocument doc) throws InterruptedException {
    // Am I the leader of a shard in "construction" state?
    String myShardId = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
    Slice mySlice = coll.getSlice(myShardId);
    if (Slice.CONSTRUCTION.equals(mySlice.getState())) {
      Replica myLeader = zkController.getZkStateReader().getLeaderRetry(collection, myShardId);
      boolean amILeader = myLeader.getName().equals(
          req.getCore().getCoreDescriptor().getCloudDescriptor()
              .getCoreNodeName());
      if (amILeader) {
        // Does the document belong to my hash range as well?
        DocRouter.Range myRange = mySlice.getRange();
        if (myRange == null) myRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (parentSlice != null)  {
          boolean isSubset = parentSlice.getRange() != null && myRange.isSubsetOf(parentSlice.getRange());
          return isSubset && coll.getRouter().isTargetSlice(id, doc, req.getParams(), myShardId, coll);
        } else  {
          // delete by query case -- as long as I am a sub shard leader we're fine
          return true;
        }
      }
    }
    return false;
  }

  private List<Node> getSubShardLeaders(DocCollection coll, String shardId, String docId, SolrInputDocument doc) {
    Collection<Slice> allSlices = coll.getSlices();
    List<Node> nodes = null;
    for (Slice aslice : allSlices) {
      if (Slice.CONSTRUCTION.equals(aslice.getState()))  {
        DocRouter.Range myRange = coll.getSlice(shardId).getRange();
        if (myRange == null) myRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        boolean isSubset = aslice.getRange() != null && aslice.getRange().isSubsetOf(myRange);
        if (isSubset &&
            (docId == null // in case of deletes
            || (docId != null && coll.getRouter().isTargetSlice(docId, doc, req.getParams(), aslice.getName(), coll)))) {
          Replica sliceLeader = aslice.getLeader();
          // slice leader can be null because node/shard is created zk before leader election
          if (sliceLeader != null && zkController.getClusterState().liveNodesContain(sliceLeader.getNodeName()))  {
            if (nodes == null) nodes = new ArrayList<Node>();
            ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(sliceLeader);
            nodes.add(new StdNode(nodeProps));
          }
        }
      }
    }
    return nodes;
  }

  private void doDefensiveChecks(DistribPhase phase) {
    boolean isReplayOrPeersync = (updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    if (isReplayOrPeersync) return;

    String from = req.getParams().get("distrib.from");
    ClusterState clusterState = zkController.getClusterState();
    CloudDescriptor cloudDescriptor = req.getCore().getCoreDescriptor().getCloudDescriptor();
    Slice mySlice = clusterState.getSlice(collection, cloudDescriptor.getShardId());
    boolean localIsLeader = cloudDescriptor.isLeader();
    if (DistribPhase.FROMLEADER == phase && localIsLeader && from != null) { // from will be null on log replay
      String fromShard = req.getParams().get("distrib.from.parent");
      if (fromShard != null) {
        if (!Slice.CONSTRUCTION.equals(mySlice.getState()))  {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but we are not in construction state");
        }
        // shard splitting case -- check ranges to see if we are a sub-shard
        Slice fromSlice = zkController.getClusterState().getCollection(collection).getSlice(fromShard);
        DocRouter.Range parentRange = fromSlice.getRange();
        if (parentRange == null) parentRange = new DocRouter.Range(Integer.MIN_VALUE, Integer.MAX_VALUE);
        if (mySlice.getRange() != null && !mySlice.getRange().isSubsetOf(parentRange)) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Request says it is coming from parent shard leader but parent hash range is not superset of my range");
        }
      } else {
        log.error("Request says it is coming from leader, but we are the leader: " + req.getParamString());
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Request says it is coming from leader, but we are the leader");
      }
    }

    if ((isLeader && !localIsLeader) || (isSubShardLeader && !localIsLeader)) {
      log.error("ClusterState says we are the leader, but locally we don't think so");
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
          "ClusterState says we are the leader (" + zkController.getBaseUrl()
              + "/" + req.getCore().getName() + "), but locally we don't think so. Request came from " + from);
    }
  }


  // used for deleteByQuery to get the list of nodes this leader should forward to
  private List<Node> setupRequest() {
    List<Node> nodes = null;
    String shardId = cloudDesc.getShardId();

    try {
      Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
          collection, shardId);
      isLeader = leaderReplica.getName().equals(
          req.getCore().getCoreDescriptor().getCloudDescriptor()
              .getCoreNodeName());

      // TODO: what if we are no longer the leader?

      forwardToLeader = false;
      List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
          .getReplicaProps(collection, shardId, leaderReplica.getName(),
              req.getCore().getName());
      if (replicaProps != null) {
        nodes = new ArrayList<Node>(replicaProps.size());
        for (ZkCoreNodeProps props : replicaProps) {
          nodes.add(new StdNode(props));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
          e);
    }

    return nodes;
  }


  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    updateCommand = cmd;

    if (zkEnabled) {
      zkCheck();
      nodes = setupRequest(cmd.getHashableId(), cmd.getSolrInputDocument());
    } else {
      isLeader = getNonZkLeaderAssumption(req);
    }

    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd = versionAdd(cmd);
    }

    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    if (zkEnabled && isLeader && !isSubShardLeader)  {
      DocCollection coll = zkController.getClusterState().getCollection(collection);
      List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), cmd.getHashableId(), cmd.getSolrInputDocument());
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set("distrib.from", ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set("distrib.from.parent", req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId());
        for (Node subShardLeader : subShardLeaders) {
          cmdDistrib.syncAdd(cmd, subShardLeader, params);
        }
      }
    }

    ModifiableSolrParams params = null;
    if (nodes != null) {

      params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_UPDATE_PARAM,
                 (isLeader || isSubShardLeader ?
                  DistribPhase.FROMLEADER.toString() :
                  DistribPhase.TOLEADER.toString()));
      if (isLeader || isSubShardLeader) {
        params.set("distrib.from", ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
      }

      params.set("distrib.from", ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));
      cmdDistrib.distribAdd(cmd, nodes, params);
    }
    
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && idField != null) {
      if (addsResponse == null) {
        addsResponse = new NamedList<String>();
        rsp.add("adds",addsResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      addsResponse.add(scratch.toString(), cmd.getVersion());
    }
    
    // TODO: keep track of errors?  needs to be done at a higher level though since
    // an id may fail before it gets to this processor.
    // Given that, it may also make sense to move the version reporting out of this
    // processor too.
  }
 
  // TODO: optionally fail if n replicas are not reached...
  private void doFinish() {
    // TODO: if not a forward and replication req is not specified, we could
    // send in a background thread

    cmdDistrib.finish();
    Response response = cmdDistrib.getResponse();
    // TODO - we may need to tell about more than one error...
    
    // if its a forward, any fail is a problem - 
    // otherwise we assume things are fine if we got it locally
    // until we start allowing min replication param
    if (response.errors.size() > 0) {
      // if one node is a RetryNode, this was a forward request
      if (response.errors.get(0).node instanceof RetryNode) {
        rsp.setException(response.errors.get(0).e);
      }
      // else
      // for now we don't error - we assume if it was added locally, we
      // succeeded 
    }
   
    
    // if it is not a forward request, for each fail, try to tell them to
    // recover - the doc was already added locally, so it should have been
    // legit

    // TODO: we should do this in the background it would seem
    for (SolrCmdDistributor.Error error : response.errors) {
      if (error.node instanceof RetryNode) {
        // we don't try to force a leader to recover
        // when we cannot forward to it
        continue;
      }
      // TODO: we should force their state to recovering ??
      // TODO: could be sent in parallel
      // TODO: do retries??
      // TODO: what if its is already recovering? Right now recoveries queue up -
      // should they?
      String recoveryUrl = error.node.getBaseUrl();
      HttpSolrServer server;
      log.info("try and ask " + recoveryUrl + " to recover");
      try {
        server = new HttpSolrServer(recoveryUrl);
        server.setSoTimeout(15000);
        server.setConnectionTimeout(15000);
        
        RequestRecovery recoverRequestCmd = new RequestRecovery();
        recoverRequestCmd.setAction(CoreAdminAction.REQUESTRECOVERY);
        recoverRequestCmd.setCoreName(error.node.getCoreName());
        
        server.request(recoverRequestCmd);
      } catch (Exception e) {
        log.info("Could not tell a replica to recover", e);
      }
      
    }
  }

 
  // must be synchronized by bucket
  private void doLocalAdd(AddUpdateCommand cmd) throws IOException {
    super.processAdd(cmd);
  }

  // must be synchronized by bucket
  private void doLocalDelete(DeleteUpdateCommand cmd) throws IOException {
    super.processDelete(cmd);
  }

  /**
   * @return whether or not to drop this cmd
   * @throws IOException If there is a low-level I/O error.
   */
  private boolean versionAdd(AddUpdateCommand cmd) throws IOException {
    BytesRef idBytes = cmd.getIndexedId();

    if (idBytes == null) {
      super.processAdd(cmd);
      return false;
    }

    if (vinfo == null) {
      if (isAtomicUpdate(cmd)) {
        throw new SolrException
          (SolrException.ErrorCode.BAD_REQUEST,
           "Atomic document updates are not supported unless <updateLog/> is configured");
      } else {
        super.processAdd(cmd);
        return false;
      }
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash here)
    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find any existing version in the document
    // TODO: don't reuse update commands any more!
    long versionOnUpdate = cmd.getVersion();

    if (versionOnUpdate == 0) {
      SolrInputField versionField = cmd.getSolrInputDocument().getField(VersionInfo.VERSION_FIELD);
      if (versionField != null) {
        Object o = versionField.getValue();
        versionOnUpdate = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString());
      } else {
        // Find the version
        String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
        versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
      }
    }

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;


    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    try {
      synchronized (bucket) {
        // we obtain the version when synchronized and then do the add so we can ensure that
        // if version1 < version2 then version1 is actually added before version2.

        // even if we don't store the version field, synchronizing on the bucket
        // will enable us to know what version happened first, and thus enable
        // realtime-get to work reliably.
        // TODO: if versions aren't stored, do we need to set on the cmd anyway for some reason?
        // there may be other reasons in the future for a version on the commands

        boolean checkDeleteByQueries = false;

        if (versionsStored) {

          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            boolean updated = getUpdatedDocument(cmd, versionOnUpdate);

            if (versionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if ( versionOnUpdate == foundVersion || (versionOnUpdate < 0 && foundVersion < 0) || (versionOnUpdate==1 && foundVersion > 0) ) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getPrintableId() + " expected=" + versionOnUpdate + " actual=" + foundVersion);
              }
            }


            long version = vinfo.getNewClock();
            cmd.setVersion(version);
            cmd.getSolrInputDocument().setField(VersionInfo.VERSION_FIELD, version);
            bucket.updateHighest(version);
          } else {
            // The leader forwarded us this update.
            cmd.setVersion(versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.add(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occurred.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }

              // also need to re-apply newer deleteByQuery commands
              checkDeleteByQueries = true;
            }
          }
        }
        
        boolean willDistrib = isLeader && nodes != null && nodes.size() > 0;
        
        SolrInputDocument clonedDoc = null;
        if (willDistrib) {
          clonedDoc = cmd.solrDoc.deepCopy();
        }

        // TODO: possibly set checkDeleteByQueries as a flag on the command?
        doLocalAdd(cmd);
        
        if (willDistrib) {
          cmd.solrDoc = clonedDoc;
        }

      }  // end synchronized (bucket)
    } finally {
      vinfo.unlockForUpdate();
    }
    return false;
  }

  /**
   * Utility method that examines the SolrInputDocument in an AddUpdateCommand
   * and returns true if the documents contains atomic update instructions.
   */
  public static boolean isAtomicUpdate(final AddUpdateCommand cmd) {
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    for (SolrInputField sif : sdoc.values()) {
      if (sif.getValue() instanceof Map) {
        return true;
      }
    }
    return false;
  }

  // TODO: may want to switch to using optimistic locking in the future for better concurrency
  // that's why this code is here... need to retry in a loop closely around/in versionAdd
  boolean getUpdatedDocument(AddUpdateCommand cmd, long versionOnUpdate) throws IOException {
    if (!isAtomicUpdate(cmd)) return false;

    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    BytesRef id = cmd.getIndexedId();
    SolrInputDocument oldDoc = RealTimeGetComponent.getInputDocument(cmd.getReq().getCore(), id);

    if (oldDoc == null) {
      // create a new doc by default if an old one wasn't found
      if (versionOnUpdate <= 0) {
        oldDoc = new SolrInputDocument();
      } else {
        // could just let the optimistic locking throw the error
        throw new SolrException(ErrorCode.CONFLICT, "Document not found for update.  id=" + cmd.getPrintableId());
      }
    } else {
      oldDoc.remove(VERSION_FIELD);
    }

    IndexSchema schema = cmd.getReq().getSchema();
    for (SolrInputField sif : sdoc.values()) {
      Object val = sif.getValue();
      if (val instanceof Map) {
        for (Entry<String,Object> entry : ((Map<String,Object>) val).entrySet()) {
          String key = entry.getKey();
          Object fieldVal = entry.getValue();
          boolean updateField = false;
          if ("add".equals(key)) {
            updateField = true;
            oldDoc.addField( sif.getName(), fieldVal, sif.getBoost());
          } else if ("set".equals(key)) {
            updateField = true;
            oldDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
          } else if ("inc".equals(key)) {
            updateField = true;
            SolrInputField numericField = oldDoc.get(sif.getName());
            if (numericField == null) {
              oldDoc.setField(sif.getName(),  fieldVal, sif.getBoost());
            } else {
              // TODO: fieldtype needs externalToObject?
              String oldValS = numericField.getFirstValue().toString();
              SchemaField sf = schema.getField(sif.getName());
              BytesRef term = new BytesRef();
              sf.getType().readableToIndexed(oldValS, term);
              Object oldVal = sf.getType().toObject(sf, term);

              String fieldValS = fieldVal.toString();
              Number result;
              if (oldVal instanceof Long) {
                result = ((Long) oldVal).longValue() + Long.parseLong(fieldValS);
              } else if (oldVal instanceof Float) {
                result = ((Float) oldVal).floatValue() + Float.parseFloat(fieldValS);
              } else if (oldVal instanceof Double) {
                result = ((Double) oldVal).doubleValue() + Double.parseDouble(fieldValS);
              } else {
                // int, short, byte
                result = ((Integer) oldVal).intValue() + Integer.parseInt(fieldValS);
              }

              oldDoc.setField(sif.getName(),  result, sif.getBoost());
            }

          }

          // validate that the field being modified is not the id field.
          if (updateField && idField.getName().equals(sif.getName())) {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid update of id field: " + sif);
          }

        }
      } else {
        // normal fields are treated as a "set"
        oldDoc.put(sif.getName(), sif);
      }

    }

    cmd.solrDoc = oldDoc;
    return true;
  }



  
  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    updateCommand = cmd;

    if (!cmd.isDeleteById()) {
      doDeleteByQuery(cmd);
      return;
    }

    if (zkEnabled) {
      zkCheck();
      nodes = setupRequest(cmd.getId(), null);
    } else {
      isLeader = getNonZkLeaderAssumption(req);
    }
    
    boolean dropCmd = false;
    if (!forwardToLeader) {
      dropCmd  = versionDelete(cmd);
    }
    
    if (dropCmd) {
      // TODO: do we need to add anything to the response?
      return;
    }

    if (zkEnabled && isLeader && !isSubShardLeader)  {
      DocCollection coll = zkController.getClusterState().getCollection(collection);
      List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), null, null);
      // the list<node> will actually have only one element for an add request
      if (subShardLeaders != null && !subShardLeaders.isEmpty()) {
        ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
        params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
        params.set("distrib.from", ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
        params.set("distrib.from.parent", cloudDesc.getShardId());
        cmdDistrib.syncDelete(cmd, subShardLeaders, params);
      }
    }


    ModifiableSolrParams params = null;
    if (nodes != null) {

      params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(DISTRIB_UPDATE_PARAM,
                 (isLeader || isSubShardLeader ?
                  DistribPhase.FROMLEADER.toString() :
                  DistribPhase.TOLEADER.toString()));
      if (isLeader || isSubShardLeader) {
        params.set("distrib.from", ZkCoreNodeProps.getCoreUrl(
            zkController.getBaseUrl(), req.getCore().getName()));
      }
      cmdDistrib.distribDelete(cmd, nodes, params);
    }

    // cmd.getIndexId == null when delete by query
    // TODO: what to do when no idField?
    if (returnVersions && rsp != null && cmd.getIndexedId() != null && idField != null) {
      if (deleteResponse == null) {
        deleteResponse = new NamedList<String>();
        rsp.add("deletes",deleteResponse);
      }
      if (scratch == null) scratch = new CharsRef();
      idField.getType().indexedToReadable(cmd.getIndexedId(), scratch);
      deleteResponse.add(scratch.toString(), cmd.getVersion());  // we're returning the version of the delete.. not the version of the doc we deleted.
    }
  }

  private ModifiableSolrParams filterParams(SolrParams params) {
    ModifiableSolrParams fparams = new ModifiableSolrParams();
    passParam(params, fparams, UpdateParams.UPDATE_CHAIN);
    passParam(params, fparams, TEST_DISTRIB_SKIP_SERVERS);
    return fparams;
  }

  private void passParam(SolrParams params, ModifiableSolrParams fparams, String param) {
    String[] values = params.getParams(param);
    if (values != null) {
      for (String value : values) {
        fparams.add(param, value);
      }
    }
  }

  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // even in non zk mode, tests simulate updates from a leader
    if(!zkEnabled) {
      isLeader = getNonZkLeaderAssumption(req);
    } else {
      zkCheck();
    }

    // NONE: we are the first to receive this deleteByQuery
    //       - it must be forwarded to the leader of every shard
    // TO:   we are a leader receiving a forwarded deleteByQuery... we must:
    //       - block all updates (use VersionInfo)
    //       - flush *all* updates going to our replicas
    //       - forward the DBQ to our replicas and wait for the response
    //       - log + execute the local DBQ
    // FROM: we are a replica receiving a DBQ from our leader
    //       - log + execute the local DBQ
    DistribPhase phase = 
    DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    DocCollection coll = zkEnabled 
      ? zkController.getClusterState().getCollection(collection) : null;

    if (zkEnabled && DistribPhase.NONE == phase) {
      boolean leaderForAnyShard = false;  // start off by assuming we are not a leader for any shard

      ModifiableSolrParams outParams = new ModifiableSolrParams(filterParams(req.getParams()));
      outParams.set(DISTRIB_UPDATE_PARAM, DistribPhase.TOLEADER.toString());

      SolrParams params = req.getParams();
      Collection<Slice> slices = coll.getRouter().getSearchSlices(params.get(ShardParams.SHARD_KEYS), params, coll);

      List<Node> leaders =  new ArrayList<Node>(slices.size());
      for (Slice slice : slices) {
        String sliceName = slice.getName();
        Replica leader;
        try {
          leader = zkController.getZkStateReader().getLeaderRetry(collection, sliceName);
        } catch (InterruptedException e) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Exception finding leader for shard " + sliceName, e);
        }

        // TODO: What if leaders changed in the meantime?
        // should we send out slice-at-a-time and if a node returns "hey, I'm not a leader" (or we get an error because it went down) then look up the new leader?

        // Am I the leader for this slice?
        ZkCoreNodeProps coreLeaderProps = new ZkCoreNodeProps(leader);
        String leaderCoreNodeName = leader.getName();
        String coreNodeName = req.getCore().getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
        isLeader = coreNodeName.equals(leaderCoreNodeName);

        if (isLeader) {
          // don't forward to ourself
          leaderForAnyShard = true;
        } else {
          leaders.add(new StdNode(coreLeaderProps));
        }
      }

      outParams.remove("commit"); // this will be distributed from the local commit
      cmdDistrib.distribDelete(cmd, leaders, outParams);

      if (!leaderForAnyShard) {
        return;
      }

      // change the phase to TOLEADER so we look up and forward to our own replicas (if any)
      phase = DistribPhase.TOLEADER;
    }

    List<Node> replicas = null;

    if (zkEnabled && DistribPhase.TOLEADER == phase) {
      // This core should be a leader
      isLeader = true;
      replicas = setupRequest();
    } else if (DistribPhase.FROMLEADER == phase) {
      isLeader = false;
    }

    if (vinfo == null) {
      super.processDelete(cmd);
      return;
    }

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;

    if (!leaderLogic && versionOnUpdate==0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    vinfo.blockUpdates();
    try {

      if (versionsStored) {
        if (leaderLogic) {
          long version = vinfo.getNewClock();
          cmd.setVersion(-version);
          // TODO update versions in all buckets

          doLocalDelete(cmd);

        } else {
          cmd.setVersion(-versionOnUpdate);

          if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
            // we're not in an active state, and this update isn't from a replay, so buffer it.
            cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
            ulog.deleteByQuery(cmd);
            return;
          }

          doLocalDelete(cmd);
        }
      }

      // since we don't know which documents were deleted, the easiest thing to do is to invalidate
      // all real-time caches (i.e. UpdateLog) which involves also getting a new version of the IndexReader
      // (so cache misses will see up-to-date data)

    } finally {
      vinfo.unblockUpdates();
    }

    if (zkEnabled)  {
      // forward to all replicas
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      params.set(VERSION_FIELD, Long.toString(cmd.getVersion()));
      params.set(DISTRIB_UPDATE_PARAM, DistribPhase.FROMLEADER.toString());
      params.set("update.from", ZkCoreNodeProps.getCoreUrl(
          zkController.getBaseUrl(), req.getCore().getName()));

      boolean someReplicas = false;
      boolean subShardLeader = false;
      try {
        subShardLeader = amISubShardLeader(coll, null, null, null);
        if (subShardLeader)  {
          String myShardId = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
          Replica leaderReplica = zkController.getZkStateReader().getLeaderRetry(
              collection, myShardId);
          List<ZkCoreNodeProps> replicaProps = zkController.getZkStateReader()
              .getReplicaProps(collection, myShardId, leaderReplica.getName(),
                  req.getCore().getName(), null, ZkStateReader.DOWN);
          if (replicaProps != null) {
            List<Node> myReplicas = new ArrayList<Node>();
            for (ZkCoreNodeProps replicaProp : replicaProps) {
              myReplicas.add(new StdNode(replicaProp));
            }
            cmdDistrib.distribDelete(cmd, myReplicas, params);
            someReplicas = true;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
      }

      if (leaderLogic) {
        List<Node> subShardLeaders = getSubShardLeaders(coll, cloudDesc.getShardId(), null, null);
        if (subShardLeaders != null)  {
          cmdDistrib.syncDelete(cmd, subShardLeaders, params);
        }
        if (replicas != null) {
          cmdDistrib.distribDelete(cmd, replicas, params);
          someReplicas = true;
        }
      }

      if (someReplicas)  {
        cmdDistrib.finish();
      }
    }


    if (returnVersions && rsp != null) {
      if (deleteByQueryResponse == null) {
        deleteByQueryResponse = new NamedList<String>();
        rsp.add("deleteByQuery",deleteByQueryResponse);
      }
      deleteByQueryResponse.add(cmd.getQuery(), cmd.getVersion());
    }
  }


  private void zkCheck() {
    if ((updateCommand.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0) {
      // for log reply or peer sync, we don't need to be connected to ZK
      return;
    }

    if (zkController.isConnected()) {
      return;
    }
    
    long timeoutAt = System.currentTimeMillis() + zkController.getClientTimeout();
    while (System.currentTimeMillis() < timeoutAt) {
      if (zkController.isConnected()) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Cannot talk to ZooKeeper - Updates are disabled.");
  }

  private boolean versionDelete(DeleteUpdateCommand cmd) throws IOException {

    BytesRef idBytes = cmd.getIndexedId();

    if (vinfo == null || idBytes == null) {
      super.processDelete(cmd);
      return false;
    }

    // This is only the hash for the bucket, and must be based only on the uniqueKey (i.e. do not use a pluggable hash here)
    int bucketHash = Hash.murmurhash3_x86_32(idBytes.bytes, idBytes.offset, idBytes.length, 0);

    // at this point, there is an update we need to try and apply.
    // we may or may not be the leader.

    // Find the version
    long versionOnUpdate = cmd.getVersion();
    if (versionOnUpdate == 0) {
      String versionOnUpdateS = req.getParams().get(VERSION_FIELD);
      versionOnUpdate = versionOnUpdateS == null ? 0 : Long.parseLong(versionOnUpdateS);
    }
    long signedVersionOnUpdate = versionOnUpdate;
    versionOnUpdate = Math.abs(versionOnUpdate);  // normalize to positive version

    boolean isReplayOrPeersync = (cmd.getFlags() & (UpdateCommand.REPLAY | UpdateCommand.PEER_SYNC)) != 0;
    boolean leaderLogic = isLeader && !isReplayOrPeersync;

    if (!leaderLogic && versionOnUpdate==0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "missing _version_ on update from leader");
    }

    VersionBucket bucket = vinfo.bucket(bucketHash);

    vinfo.lockForUpdate();
    try {

      synchronized (bucket) {
        if (versionsStored) {
          long bucketVersion = bucket.highest;

          if (leaderLogic) {

            if (signedVersionOnUpdate != 0) {
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              long foundVersion = lastVersion == null ? -1 : lastVersion;
              if ( (signedVersionOnUpdate == foundVersion) || (signedVersionOnUpdate < 0 && foundVersion < 0) || (signedVersionOnUpdate == 1 && foundVersion > 0) ) {
                // we're ok if versions match, or if both are negative (all missing docs are equal), or if cmd
                // specified it must exist (versionOnUpdate==1) and it does.
              } else {
                throw new SolrException(ErrorCode.CONFLICT, "version conflict for " + cmd.getId() + " expected=" + signedVersionOnUpdate + " actual=" + foundVersion);
              }
            }

            long version = vinfo.getNewClock();
            cmd.setVersion(-version);
            bucket.updateHighest(version);
          } else {
            cmd.setVersion(-versionOnUpdate);

            if (ulog.getState() != UpdateLog.State.ACTIVE && (cmd.getFlags() & UpdateCommand.REPLAY) == 0) {
              // we're not in an active state, and this update isn't from a replay, so buffer it.
              cmd.setFlags(cmd.getFlags() | UpdateCommand.BUFFERING);
              ulog.delete(cmd);
              return true;
            }

            // if we aren't the leader, then we need to check that updates were not re-ordered
            if (bucketVersion != 0 && bucketVersion < versionOnUpdate) {
              // we're OK... this update has a version higher than anything we've seen
              // in this bucket so far, so we know that no reordering has yet occured.
              bucket.updateHighest(versionOnUpdate);
            } else {
              // there have been updates higher than the current update.  we need to check
              // the specific version for this id.
              Long lastVersion = vinfo.lookupVersion(cmd.getIndexedId());
              if (lastVersion != null && Math.abs(lastVersion) >= versionOnUpdate) {
                // This update is a repeat, or was reordered.  We need to drop this update.
                return true;
              }
            }
          }
        }

        doLocalDelete(cmd);
        return false;
      }  // end synchronized (bucket)

    } finally {
      vinfo.unlockForUpdate();
    }
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    updateCommand = cmd;
    List<Node> nodes = null;
    boolean singleLeader = false;
    if (zkEnabled) {
      zkCheck();
      
      nodes = getCollectionUrls(req, req.getCore().getCoreDescriptor()
          .getCloudDescriptor().getCollectionName());
      if (isLeader && nodes.size() == 1) {
        singleLeader = true;
      }
    }
    
    if (!zkEnabled || req.getParams().getBool(COMMIT_END_POINT, false) || singleLeader) {
      doLocalCommit(cmd);
    } else if (zkEnabled) {
      ModifiableSolrParams params = new ModifiableSolrParams(filterParams(req.getParams()));
      if (!req.getParams().getBool(COMMIT_END_POINT, false)) {
        params.set(COMMIT_END_POINT, true);

        if (nodes != null) {
          cmdDistrib.distribCommit(cmd, nodes, params);
          finish();
        }
      }
    }
  }

  private void doLocalCommit(CommitUpdateCommand cmd) throws IOException {
    if (vinfo != null) {
      vinfo.lockForUpdate();
    }
    try {

      if (ulog == null || ulog.getState() == UpdateLog.State.ACTIVE || (cmd.getFlags() & UpdateCommand.REPLAY) != 0) {
        super.processCommit(cmd);
      } else {
        log.info("Ignoring commit while not ACTIVE - state: " + ulog.getState() + " replay:" + (cmd.getFlags() & UpdateCommand.REPLAY));
      }

    } finally {
      if (vinfo != null) {
        vinfo.unlockForUpdate();
      }
    }
  }
  
  @Override
  public void finish() throws IOException {
    if (zkEnabled) doFinish();
    
    if (next != null && nodes == null) next.finish();
  }
 

  
  private List<Node> getCollectionUrls(SolrQueryRequest req, String collection) {
    ClusterState clusterState = req.getCore().getCoreDescriptor()
        .getCoreContainer().getZkController().getClusterState();
    List<Node> urls = new ArrayList<Node>();
    Map<String,Slice> slices = clusterState.getSlicesMap(collection);
    if (slices == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + clusterState);
    }
    for (Map.Entry<String,Slice> sliceEntry : slices.entrySet()) {
      Slice replicas = slices.get(sliceEntry.getKey());

      Map<String,Replica> shardMap = replicas.getReplicasMap();
      
      for (Entry<String,Replica> entry : shardMap.entrySet()) {
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
        if (clusterState.liveNodesContain(nodeProps.getNodeName())) {
          urls.add(new StdNode(nodeProps));
        }
      }
    }
    if (urls.size() == 0) {
      return null;
    }
    return urls;
  }
  
  // RetryNodes are used in the case of 'forward to leader' where we want
  // to try the latest leader on a fail in the case the leader just went down.
  public static class RetryNode extends StdNode {
    
    private ZkStateReader zkStateReader;
    private String collection;
    private String shardId;
    
    public RetryNode(ZkCoreNodeProps nodeProps, ZkStateReader zkStateReader, String collection, String shardId) {
      super(nodeProps);
      this.zkStateReader = zkStateReader;
      this.collection = collection;
      this.shardId = shardId;
    }

    @Override
    public boolean checkRetry() {
      ZkCoreNodeProps leaderProps;
      try {
        leaderProps = new ZkCoreNodeProps(zkStateReader.getLeaderRetry(
            collection, shardId));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      
      this.nodeProps = leaderProps;

      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result + ((shardId == null) ? 0 : shardId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!super.equals(obj)) return false;
      if (getClass() != obj.getClass()) return false;
      RetryNode other = (RetryNode) obj;
      if (nodeProps.getCoreUrl() == null) {
        if (other.nodeProps.getCoreUrl() != null) return false;
      } else if (!nodeProps.getCoreUrl().equals(other.nodeProps.getCoreUrl())) return false;

      return true;
    }
  }

  /**
   * Returns a boolean indicating whether or not the caller should behave as
   * if this is the "leader" even when ZooKeeper is not enabled.  
   * (Even in non zk mode, tests may simulate updates to/from a leader)
   */
  public static boolean getNonZkLeaderAssumption(SolrQueryRequest req) {
    DistribPhase phase = 
      DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));

    // if we have been told we are coming from a leader, then we are 
    // definitely not the leader.  Otherwise assume we are.
    return DistribPhase.FROMLEADER != phase;
  }
}
