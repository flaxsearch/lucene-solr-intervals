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

package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Abstraction through which all communication with a Solr server may be routed
 *
 * @since 5.0, replaced {@code SolrServer}
 */
public abstract class SolrClient implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;
  private DocumentObjectBinder binder;

  /**
   * Adds a collection of documents
   * @param docs  the collection of documents
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(docs, -1);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since solr 3.5
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Adds a collection of beans
   * @param beans  the collection of beans
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
    return addBeans(beans, -1);
  }

  /**
   * Adds a collection of beans specifying max time before they become committed
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since solr 3.5
   */
  public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    DocumentObjectBinder binder = this.getBinder();
    ArrayList<SolrInputDocument> docs =  new ArrayList<>(beans.size());
    for (Object bean : beans) {
      docs.add(binder.toSolrInputDocument(bean));
    }
    return add(docs, commitWithinMs);
  }

  /**
   * Adds a single document
   * @param doc  the input document
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    return add(doc, -1);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since solr 3.5
   */
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Adds a single bean
   * @param obj  the input bean
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    return addBean(obj, -1);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   * @param obj  the input bean
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since solr 3.5
   */
  public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return add(getBinder().toSolrInputDocument(obj),commitWithinMs);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse commit() throws SolrServerException, IOException {
    return commit(true, true);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse optimize() throws SolrServerException, IOException {
    return optimize(true, true, 1);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return new UpdateRequest().setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher).process( this );
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files nor writing a new index descriptor
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
    return new UpdateRequest().setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher, softCommit).process( this );
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(waitFlush, waitSearcher, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the main query searcher, making the changes visible 
   * @param maxSegments  optimizes down to at most this number of segments
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments) throws SolrServerException, IOException {
    return new UpdateRequest().setAction(UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments).process( this );
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return new UpdateRequest().rollback().process( this );
  }

  /**
   * Deletes a single document by unique ID
   * @param id  the ID of the document to delete
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return deleteById(id, -1);
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Deletes a list of documents by unique ID
   * @param ids  the list of document IDs to delete 
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    return deleteById(ids, -1);
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit
   * @param ids  the list of document IDs to delete 
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(ids);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Deletes documents from the index based on a query
   * @param query  the query expressing what documents to delete
   * @throws IOException If there is a low-level I/O error.
   */
  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    return deleteByQuery(query, -1);
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen 
   * @throws IOException If there is a low-level I/O error.
   * @since 3.6
   */
  public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(query);
    req.setCommitWithin(commitWithinMs);
    return req.process(this);
  }

  /**
   * Issues a ping request to check if the server is alive
   * @throws IOException If there is a low-level I/O error.
   */
  public SolrPingResponse ping() throws SolrServerException, IOException {
    return new SolrPing().process(this);
  }

  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   */
  public QueryResponse query(SolrParams params) throws SolrServerException, IOException {
    return new QueryRequest(params).process(this);
  }

  /**
   * Performs a query to the Solr server
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   */
  public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException, IOException {
    return new QueryRequest(params, method).process(this);
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will 
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return 
   * the results in the QueryResponse.
   *
   * @since solr 4.0
   */
  public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback) throws SolrServerException, IOException
  {
    ResponseParser parser = new StreamingBinaryResponseParser(callback);
    QueryRequest req = new QueryRequest(params);
    req.setStreamingResponseCallback(callback);
    req.setResponseParser(parser);
    return req.process(this);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier.
   *
   * @return retrieved SolrDocument, null if no document is found.
   */
  public SolrDocument getById(String id) throws SolrServerException, IOException {
    return getById(id, null);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier and uses
   * the SolrParams to execute the request.
   *
   * @return retrieved SolrDocument, null if no document is found.
   */
  public SolrDocument getById(String id, SolrParams params) throws SolrServerException, IOException {
    SolrDocumentList docs = getById(Arrays.asList(id), params);
    if (!docs.isEmpty()) {
      return docs.get(0);
    }
    return null;
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers.
   * If a document was not found, it will not be added to the SolrDocumentList.
   */
  public SolrDocumentList getById(Collection<String> ids) throws SolrServerException, IOException {
    return getById(ids, null);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers and uses
   * the SolrParams to execute the request.
   * If a document was not found, it will not be added to the SolrDocumentList.
   */
  public SolrDocumentList getById(Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("Must provide an identifier of a document to retrieve.");
    }

    ModifiableSolrParams reqParams = new ModifiableSolrParams(params);
    if (StringUtils.isEmpty(reqParams.get(CommonParams.QT))) {
      reqParams.set(CommonParams.QT, "/get");
    }
    reqParams.set("ids", (String[]) ids.toArray());

    return query(reqParams).getResults();
  }
  
  /**
   * SolrServer implementations need to implement how a request is actually processed
   */
  public abstract NamedList<Object> request(final SolrRequest request) throws SolrServerException, IOException;

  public DocumentObjectBinder getBinder() {
    if(binder == null){
      binder = new DocumentObjectBinder();
    }
    return binder;
  }

  /**
   * Release allocated resources.
   *
   * @since solr 4.0
   * @deprecated Use close() instead.
   */
  @Deprecated
  public abstract void shutdown();

  //@SuppressWarnings("deprecation")
  public void close() throws IOException {
    shutdown();
  }
}
