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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

/**
 * Provides methods for marshalling an UpdateRequest to a NamedList which can be serialized in the javabin format and
 * vice versa.
 *
 *
 * @see org.apache.solr.common.util.JavaBinCodec
 * @since solr 1.4
 */
public class JavaBinUpdateRequestCodec {

  /**
   * Converts an UpdateRequest to a NamedList which can be serialized to the given OutputStream in the javabin format
   *
   * @param updateRequest the UpdateRequest to be written out
   * @param os            the OutputStream to which the request is to be written
   *
   * @throws IOException in case of an exception during marshalling or writing to the stream
   */
  public void marshal(UpdateRequest updateRequest, OutputStream os) throws IOException {
    NamedList nl = new NamedList();
    NamedList params = solrParamsToNamedList(updateRequest.getParams());
    if (updateRequest.getCommitWithin() != -1) {
      params.add("commitWithin", updateRequest.getCommitWithin());
    }
    Iterator<SolrInputDocument> docIter = null;

    if (updateRequest.getDocuments() != null) {
      docIter = updateRequest.getDocuments().iterator();
    }
    if(updateRequest.getDocIterator() != null){
      docIter = updateRequest.getDocIterator();
    }

    nl.add("params", params);// 0: params
    nl.add("delById", updateRequest.getDeleteById());
    nl.add("delByQ", updateRequest.getDeleteQuery());
    nl.add("docs", docIter);
    JavaBinCodec codec = new JavaBinCodec();
    codec.marshal(nl, os);
  }

  /**
   * Reads a NamedList from the given InputStream, converts it into a SolrInputDocument and passes it to the given
   * StreamingUpdateHandler
   *
   * @param is      the InputStream from which to read
   * @param handler an instance of StreamingUpdateHandler to which SolrInputDocuments are streamed one by one
   *
   * @return the UpdateRequest
   *
   * @throws IOException in case of an exception while reading from the input stream or unmarshalling
   */
  public UpdateRequest unmarshal(InputStream is, final StreamingUpdateHandler handler) throws IOException {
    final UpdateRequest updateRequest = new UpdateRequest();
    List<List<NamedList>> doclist;
    List<String> delById;
    List<String> delByQ;
    final NamedList[] namedList = new NamedList[1];
    JavaBinCodec codec = new JavaBinCodec() {

      // NOTE: this only works because this is an anonymous inner class 
      // which will only ever be used on a single stream -- if this class 
      // is ever refactored, this will not work.
      private boolean seenOuterMostDocIterator = false;
        
      @Override
      public NamedList readNamedList(DataInputInputStream dis) throws IOException {
        int sz = readSize(dis);
        NamedList nl = new NamedList();
        if (namedList[0] == null) {
          namedList[0] = nl;
        }
        for (int i = 0; i < sz; i++) {
          String name = (String) readVal(dis);
          Object val = readVal(dis);
          nl.add(name, val);
        }
        return nl;
      }

      @Override
      public List readIterator(DataInputInputStream fis) throws IOException {

        // default behavior for reading any regular Iterator in the stream
        if (seenOuterMostDocIterator) return super.readIterator(fis);

        // special treatment for first outermost Iterator 
        // (the list of documents)
        seenOuterMostDocIterator = true;
        return readOuterMostDocIterator(fis);
      }

      private List readOuterMostDocIterator(DataInputInputStream fis) throws IOException {
        NamedList params = (NamedList) namedList[0].getVal(0);
        updateRequest.setParams(new ModifiableSolrParams(SolrParams.toSolrParams(params)));
        if (handler == null) return super.readIterator(fis);
        while (true) {
          Object o = readVal(fis);
          if (o == END_OBJ) break;
          SolrInputDocument sdoc = null;
          if (o instanceof List) {
            sdoc = listToSolrInputDocument((List<NamedList>) o);
          } else if (o instanceof NamedList)  {
            UpdateRequest req = new UpdateRequest();
            req.setParams(new ModifiableSolrParams(SolrParams.toSolrParams((NamedList) o)));
            handler.update(null, req);
          } else  {
            sdoc = (SolrInputDocument) o;
          }
          handler.update(sdoc, updateRequest);
        }
        return Collections.EMPTY_LIST;
      }
    };


    codec.unmarshal(is);
    
    // NOTE: if the update request contains only delete commands the params
    // must be loaded now
    if(updateRequest.getParams()==null) {
      NamedList params = (NamedList) namedList[0].get("params");
      if(params!=null) {
        updateRequest.setParams(new ModifiableSolrParams(SolrParams.toSolrParams(params)));
      }
    }
    delById = (List<String>) namedList[0].get("delById");
    delByQ = (List<String>) namedList[0].get("delByQ");
    doclist = (List) namedList[0].get("docs");

    if (doclist != null && !doclist.isEmpty()) {
      List<SolrInputDocument> solrInputDocs = new ArrayList<SolrInputDocument>();
      for (Object o : doclist) {
        if (o instanceof List) {
          solrInputDocs.add(listToSolrInputDocument((List<NamedList>)o));
        } else  {
          solrInputDocs.add((SolrInputDocument)o);
        }
      }
      updateRequest.add(solrInputDocs);
    }
    if (delById != null) {
      for (String s : delById) {
        updateRequest.deleteById(s);
      }
    }
    if (delByQ != null) {
      for (String s : delByQ) {
        updateRequest.deleteByQuery(s);
      }
    }
    return updateRequest;

  }

  private SolrInputDocument listToSolrInputDocument(List<NamedList> namedList) {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < namedList.size(); i++) {
      NamedList nl = namedList.get(i);
      if (i == 0) {
        doc.setDocumentBoost(nl.getVal(0) == null ? 1.0f : (Float) nl.getVal(0));
      } else {
        doc.addField((String) nl.getVal(0),
                nl.getVal(1),
                nl.getVal(2) == null ? 1.0f : (Float) nl.getVal(2));
      }
    }
    return doc;
  }

  private NamedList solrParamsToNamedList(SolrParams params) {
    if (params == null) return new NamedList();
    return params.toNamedList();
  }

  public static interface StreamingUpdateHandler {
    public void update(SolrInputDocument document, UpdateRequest req);
  }
}
