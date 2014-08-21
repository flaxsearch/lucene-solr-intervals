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

package org.apache.solr.client.solrj.request;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * This class is experimental and subject to change.
 *
 * @since solr 4.5
 */
public class CollectionAdminRequest extends SolrRequest
{
  protected String collection = null;
  protected CollectionAction action = null;
  protected String asyncId = null;

  protected static class CollectionShardAdminRequest extends CollectionAdminRequest {
    protected String shardName = null;

    public void setShardName(String shard) { this.shardName = shard; }
    public String getShardName() { return this.shardName; }

    public ModifiableSolrParams getCommonParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.remove( "name" );
      params.set( "collection", collection );
      params.set( "shard", shardName);
      params.set( "async", asyncId);
      return params;
    }

    @Override
    public SolrParams getParams() {
      return getCommonParams();
    }
  }

  //a create collection request
  public static class Create extends CollectionAdminRequest {
    protected String configName = null;
    protected String createNodeSet = null;
    protected String routerName;
    protected String shards;
    protected String routerField;
    protected Integer numShards;
    protected Integer maxShardsPerNode;
    protected Integer replicationFactor;
    protected Boolean autoAddReplicas;


    public Create() {
      action = CollectionAction.CREATE;
    }

    public void setConfigName(String config) { this.configName = config; }
    public void setCreateNodeSet(String nodeSet) { this.createNodeSet = nodeSet; }
    public void setRouterName(String routerName) { this.routerName = routerName; }
    public void setShards(String shards) { this.shards = shards; }
    public void setRouterField(String routerField) { this.routerField = routerField; }
    public void setNumShards(Integer numShards) {this.numShards = numShards;}
    public void setMaxShardsPerNode(Integer numShards) { this.maxShardsPerNode = numShards; }
    public void setAutoAddReplicas(boolean autoAddReplicas) { this.autoAddReplicas = autoAddReplicas; }
    public void setReplicationFactor(Integer repl) { this.replicationFactor = repl; }

    public String getConfigName()  { return configName; }
    public String getCreateNodeSet() { return createNodeSet; }
    public String getRouterName() { return  routerName; }
    public String getShards() { return  shards; }
    public Integer getNumShards() { return numShards; }
    public Integer getMaxShardsPerNode() { return maxShardsPerNode; }
    public Integer getReplicationFactor() { return replicationFactor; }
    public Boolean getAutoAddReplicas() { return autoAddReplicas; }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();

      if (configName != null) {
        params.set( "collection.configName", configName);
      }
      if (createNodeSet != null) {
        params.set( "createNodeSet", createNodeSet);
      }
      if (numShards != null) {
        params.set( ZkStateReader.NUM_SHARDS_PROP, numShards);
      }
      if (maxShardsPerNode != null) {
        params.set( "maxShardsPerNode", maxShardsPerNode);
      }
      if (routerName != null) {
        params.set( "router.name", routerName);
      }
      if (shards != null) {
        params.set("shards", shards);
      }
      if (routerField != null) {
        params.set("router.field", routerField);
      }
      if (replicationFactor != null) {
        // OverseerCollectionProcessor.REPLICATION_FACTOR
        params.set( "replicationFactor", replicationFactor);
      }
      if (asyncId != null) {
        params.set("async", asyncId);
      }
      if (autoAddReplicas != null) {
        params.set(ZkStateReader.AUTO_ADD_REPLICAS, autoAddReplicas);
      }

      return params;
    }
  }

  //a reload collection request
  public static class Reload extends CollectionAdminRequest {
    public Reload() {
      action = CollectionAction.RELOAD;
    }
  }

  //a delete collection request
  public static class Delete extends CollectionAdminRequest {
    public Delete() {
      action = CollectionAction.DELETE;
    }
  }

  //a create shard collection request
  public static class CreateShard extends CollectionShardAdminRequest {
    protected String nodeSet;

    public void setNodeSet(String nodeSet) { this.nodeSet = nodeSet; }
    public String getNodeSet() { return nodeSet; }

    public CreateShard() {
      action = CollectionAction.CREATESHARD;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = getCommonParams();
      params.set( "createNodeSet", nodeSet);
      return params;
    }
  }

  //a split shard collection request
  public static class SplitShard extends CollectionShardAdminRequest {
    protected String ranges;

    public void setRanges(String ranges) { this.ranges = ranges; }
    public String getRanges() { return ranges; }

    public SplitShard() {
      action = CollectionAction.SPLITSHARD;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = getCommonParams();
      params.set( "ranges", ranges);
      return params;
    }
  }

  //a delete shard collection request
  public static class DeleteShard extends CollectionShardAdminRequest {
    public DeleteShard() {
      action = CollectionAction.DELETESHARD;
    }
  }

  //a request status collection request
  public static class RequestStatus extends CollectionAdminRequest {
    protected  String requestId = null;

    public RequestStatus() {
      action = CollectionAction.REQUESTSTATUS;
    }

    public void setRequestId(String requestId) {this.requestId = requestId; }
    public String getRequestId() { return this.requestId; }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set("requestid", requestId);
      return params;
    }
  }

  //a collection alias create request
  public static class CreateAlias extends CollectionAdminRequest {
    protected String aliasedCollections = null;

    public void setAliasedCollections(String alias) { this.aliasedCollections = alias; }
    public String getAliasedCollections() { return this.aliasedCollections; }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
      params.set( "collections", aliasedCollections );
      return params;
    }


    public CreateAlias() {
      action = CollectionAction.CREATEALIAS;
    }
  }

  //a collection alias delete request
  public static class DeleteAlias extends CollectionAdminRequest {
    public DeleteAlias() {
      action = CollectionAction.DELETEALIAS;
    }
  }

  public static class AddReplica extends CollectionShardAdminRequest {
    private String node;
    private String routeKey;
    private String instanceDir;
    private String dataDir;

    public AddReplica() {
      action = CollectionAction.ADDREPLICA;
    }

    public String getNode() {
      return node;
    }

    public void setNode(String node) {
      this.node = node;
    }

    public String getRouteKey() {
      return routeKey;
    }

    public void setRouteKey(String routeKey) {
      this.routeKey = routeKey;
    }

    public String getInstanceDir() {
      return instanceDir;
    }

    public void setInstanceDir(String instanceDir) {
      this.instanceDir = instanceDir;
    }

    public String getDataDir() {
      return dataDir;
    }

    public void setDataDir(String dataDir) {
      this.dataDir = dataDir;
    }

    @Override
    public SolrParams getParams() {
      ModifiableSolrParams params = new ModifiableSolrParams(super.getParams());
      if (shardName == null || shardName.isEmpty()) {
        params.remove("shard");
        if (routeKey == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either shard or routeKey must be provided");
        }
        params.add(ShardParams._ROUTE_, routeKey);
      }
      if (node != null) {
        params.add("node", node);
      }
      if (instanceDir != null)  {
        params.add("instanceDir", instanceDir);
      }
      if (dataDir != null)  {
        params.add("dataDir", dataDir);
      }
      return params;
    }
  }

  public CollectionAdminRequest()
  {
    super( METHOD.GET, "/admin/collections" );
  }

  public CollectionAdminRequest( String path )
  {
    super( METHOD.GET, path );
  }

  public final void setCollectionName( String collectionName )
  {
    this.collection = collectionName;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  public void setAction( CollectionAction action )
  {
    this.action = action;
  }

  public void setAsyncId(String asyncId) {
    this.asyncId = asyncId;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  @Override
  public SolrParams getParams()
  {
    if( action == null ) {
      throw new RuntimeException( "no action specified!" );
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set( CoreAdminParams.ACTION, action.toString() );
    params.set( CoreAdminParams.NAME, collection );
    return params;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    return null;
  }

  @Override
  public CollectionAdminResponse process(SolrServer server) throws SolrServerException, IOException
  {
    long startTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    CollectionAdminResponse res = new CollectionAdminResponse();
    res.setResponse( server.request( this ) );
    long endTime = TimeUnit.MILLISECONDS.convert(System.nanoTime(), TimeUnit.NANOSECONDS);
    res.setElapsedTime(endTime - startTime);
    return res;
  }

  //---------------------------------------------------------------------------------------
  //
  //---------------------------------------------------------------------------------------

  // creates collection using a compositeId router
  public static CollectionAdminResponse createCollection( String name,
                                                          Integer shards, Integer repl, Integer maxShards,
                                                          String nodeSet,
                                                          String conf,
                                                          String routerField,
                                                          SolrServer server) throws SolrServerException, IOException
  {
    return createCollection(name, shards, repl, maxShards, nodeSet, conf, routerField, server, null);
  }
  
  // creates collection using a compositeId router
  public static CollectionAdminResponse createCollection( String name,
                                                          Integer shards, Integer repl, Integer maxShards,
                                                          String nodeSet,
                                                          String conf,
                                                          String routerField,
                                                          Boolean autoAddReplicas,
                                                          SolrServer server) throws SolrServerException, IOException
  {
    Create req = new Create();
    req.setCollectionName(name);
    req.setRouterName("compositeId");
    req.setNumShards(shards);
    req.setReplicationFactor(repl);
    req.setMaxShardsPerNode(maxShards);
    req.setCreateNodeSet(nodeSet);
    req.setConfigName(conf);
    req.setRouterField(routerField);
    req.setAutoAddReplicas(autoAddReplicas);
    return req.process( server );
  }

  // creates collection using a compositeId router
  public static CollectionAdminResponse createCollection( String name,
                                                          Integer shards, Integer repl, Integer maxShards,
                                                          String nodeSet,
                                                          String conf,
                                                          String routerField,
                                                          SolrServer server,
                                                          String asyncId) throws SolrServerException, IOException
  {
    Create req = new Create();
    req.setCollectionName(name);
    req.setRouterName("compositeId");
    req.setNumShards(shards);
    req.setReplicationFactor(repl);
    req.setMaxShardsPerNode(maxShards);
    req.setCreateNodeSet(nodeSet);
    req.setConfigName(conf);
    req.setRouterField(routerField);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse createCollection(String name, Integer shards,
                                                        String conf,
                                                        SolrServer server) throws SolrServerException, IOException {
    return createCollection(name, shards, conf, server, null);
  }

  public static CollectionAdminResponse createCollection( String name,
                                                          Integer shards, String conf,
                                                          SolrServer server,
                                                          String asyncId) throws SolrServerException, IOException
  {
    Create req = new Create();
    req.setCollectionName(name);
    req.setRouterName("compositeId");
    req.setNumShards(shards);
    req.setConfigName(conf);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse createCollection(String name,
                                                         String shards, Integer repl, Integer maxShards,
                                                         String nodeSet,
                                                         String conf,
                                                         String routerField,
                                                         SolrServer server) throws SolrServerException, IOException {
    return createCollection(name, shards, repl, maxShards, nodeSet, conf, routerField, server, null);
  }

  // creates a collection using an implicit router
  public static CollectionAdminResponse createCollection( String name,
                                                          String shards, Integer repl, Integer maxShards,
                                                          String nodeSet,
                                                          String conf,
                                                          String routerField,
                                                          SolrServer server,
                                                          String asyncId) throws SolrServerException, IOException
  {
    Create req = new Create();
    req.setCollectionName(name);
    req.setRouterName("implicit");
    req.setShards(shards);
    req.setReplicationFactor(repl);
    req.setMaxShardsPerNode(maxShards);
    req.setCreateNodeSet(nodeSet);
    req.setConfigName(conf);
    req.setRouterField(routerField);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse createCollection( String name,
                                                          String shards, String conf,
                                                          SolrServer server) throws SolrServerException, IOException
  {
    return createCollection(name, shards, conf, server, null);
  }

  public static CollectionAdminResponse createCollection( String name,
                                                          String shards, String conf,
                                                          SolrServer server, String asyncId ) throws SolrServerException, IOException
  {
    Create req = new Create();
    req.setCollectionName(name);
    req.setRouterName("implicit");
    req.setShards(shards);
    req.setConfigName(conf);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse reloadCollection( String name, SolrServer server)
      throws SolrServerException, IOException {
    return reloadCollection(name, server, null);
  }

  public static CollectionAdminResponse reloadCollection( String name, SolrServer server, String asyncId )
      throws SolrServerException, IOException
  {
    CollectionAdminRequest req = new Reload();
    req.setCollectionName(name);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse deleteCollection( String name, SolrServer server)
      throws SolrServerException, IOException
  {
    return deleteCollection(name, server, null);
  }

  public static CollectionAdminResponse deleteCollection( String name, SolrServer server,
                                                          String asyncId)
      throws SolrServerException, IOException
  {
    CollectionAdminRequest req = new Delete();
    req.setCollectionName(name);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse requestStatus(String requestId, SolrServer server)
      throws SolrServerException, IOException {
    RequestStatus req = new RequestStatus();

    req.setRequestId(requestId);
    return req.process(server);
  }

  public static CollectionAdminResponse createShard( String name, String shard, String nodeSet, SolrServer server ) throws SolrServerException, IOException
  {
    CreateShard req = new CreateShard();
    req.setCollectionName(name);
    req.setShardName(shard);
    req.setNodeSet(nodeSet);
    return req.process( server );
  }
  public static CollectionAdminResponse createShard( String name, String shard, SolrServer server ) throws SolrServerException, IOException
  {
    return createShard(name, shard, null, server);
  }

  public static CollectionAdminResponse splitShard( String name, String shard, String ranges, SolrServer server ) throws SolrServerException, IOException
  {
    return splitShard(name, shard, ranges, server, null);
  }

  public static CollectionAdminResponse splitShard( String name, String shard, String ranges, SolrServer server,
                                                    String asyncId) throws SolrServerException, IOException
  {
    SplitShard req = new SplitShard();
    req.setCollectionName(name);
    req.setShardName(shard);
    req.setRanges(ranges);
    req.setAsyncId(asyncId);
    return req.process( server );
  }

  public static CollectionAdminResponse splitShard(String name, String shard, SolrServer server)
      throws SolrServerException, IOException {
    return splitShard(name, shard, null, server, null);
  }

  public static CollectionAdminResponse splitShard( String name, String shard, SolrServer server,
                                                    String asyncId ) throws SolrServerException, IOException
  {
    return splitShard(name, shard, null, server, asyncId);
  }

  public static CollectionAdminResponse deleteShard( String name, String shard, SolrServer server ) throws SolrServerException, IOException
  {
    CollectionShardAdminRequest req = new DeleteShard();
    req.setCollectionName(name);
    req.setShardName(shard);
    return req.process( server );
  }

  public static CollectionAdminResponse createAlias( String name, String collections, SolrServer server ) throws SolrServerException, IOException
  {
    CreateAlias req = new CreateAlias();
    req.setCollectionName(name);
    req.setAliasedCollections(collections);
    return req.process( server );
  }

  public static CollectionAdminResponse deleteAlias( String name, SolrServer server ) throws SolrServerException, IOException
  {
    CollectionAdminRequest req = new DeleteAlias();
    req.setCollectionName(name);
    return req.process( server );
  }
}
