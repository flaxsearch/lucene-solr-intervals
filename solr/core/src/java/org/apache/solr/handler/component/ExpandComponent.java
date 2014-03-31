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

package org.apache.solr.handler.component;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.schema.FieldType;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ExpandParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
  * The ExpandComponent is designed to work with the CollapsingPostFilter.
  * The CollapsingPostFilter collapses a result set on a field.
  * <p/>
  * The ExpandComponent expands the collapsed groups for a single page.
  * <p/>
  * http parameters:
  * <p/>
  * expand=true <br/>
  * expand.rows=5 </br>
  * expand.sort=field asc|desc
  *
  **/
    
public class ExpandComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware {
  public static final String COMPONENT_NAME = "expand";
  private PluginInfo info = PluginInfo.EMPTY_INFO;

      @Override
  public void init(PluginInfo info) {
      this.info = info;
  }

      @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(ExpandParams.EXPAND,false)) {
      rb.doExpand = true;
    }
  }
      @Override
  public void inform(SolrCore core) {

  }

        @Override
  public void process(ResponseBuilder rb) throws IOException {

    if(!rb.doExpand) {
      return;
    }

    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    String ids = params.get(ShardParams.IDS);

    if(ids == null && isShard) {
      return;
    }

    String field = null;
    String sortParam = params.get(ExpandParams.EXPAND_SORT);
    int limit = params.getInt(ExpandParams.EXPAND_ROWS, 5);

    Sort sort = null;

    if(sortParam != null) {
      sort = QueryParsing.parseSortSpec(sortParam, rb.req).getSort();
    }

    Query query = rb.getQuery();
    List<Query> filters = rb.getFilters();
    List<Query> newFilters = new ArrayList();
    for(Query q : filters) {
      if(!(q instanceof CollapsingQParserPlugin.CollapsingPostFilter)) {
        newFilters.add(q);
      } else {
        CollapsingQParserPlugin.CollapsingPostFilter cp = (CollapsingQParserPlugin.CollapsingPostFilter)q;
        field = cp.getField();
      }
    }

    if(field == null) {
      throw new IOException("Expand field is null.");
    }

    SolrIndexSearcher searcher = req.getSearcher();
    AtomicReader reader = searcher.getAtomicReader();
    SortedDocValues values = FieldCache.DEFAULT.getTermsIndex(reader, field);
    FixedBitSet groupBits = new FixedBitSet(values.getValueCount());
    DocList docList = rb.getResults().docList;
    IntOpenHashSet collapsedSet = new IntOpenHashSet(docList.size()*2);

    DocIterator idit = docList.iterator();

    while(idit.hasNext()) {
      int doc = idit.nextDoc();
      int ord = values.getOrd(doc);
      if(ord > -1) {
        groupBits.set(ord);
        collapsedSet.add(doc);
      }
    }

    Collector collector = null;
    GroupExpandCollector groupExpandCollector = new GroupExpandCollector(values, groupBits, collapsedSet, limit, sort);
    SolrIndexSearcher.ProcessedFilter pfilter = searcher.getProcessedFilter(null, newFilters);
    if(pfilter.postFilter != null) {
      pfilter.postFilter.setLastDelegate(groupExpandCollector);
      collector = pfilter.postFilter;
    } else {
      collector = groupExpandCollector;
    }

    searcher.search(query, pfilter.filter, collector);
    IntObjectOpenHashMap groups = groupExpandCollector.getGroups();
    Iterator<IntObjectCursor> it = groups.iterator();
    Map<String, DocSlice> outMap = new HashMap();
    BytesRef bytesRef = new BytesRef();
    CharsRef charsRef = new CharsRef();
    FieldType fieldType = searcher.getSchema().getField(field).getType();

    while(it.hasNext()) {
      IntObjectCursor cursor = it.next();
      int ord = cursor.key;
      TopDocsCollector topDocsCollector = (TopDocsCollector)cursor.value;
      TopDocs topDocs = topDocsCollector.topDocs();
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      if(scoreDocs.length > 0) {
        int[] docs = new int[scoreDocs.length];
        float[] scores = new float[scoreDocs.length];
        for(int i=0; i<docs.length; i++) {
          ScoreDoc scoreDoc = scoreDocs[i];
          docs[i] = scoreDoc.doc;
          scores[i] = scoreDoc.score;
        }
        DocSlice slice = new DocSlice(0, docs.length, docs, scores, topDocs.totalHits, topDocs.getMaxScore());
        values.lookupOrd(ord, bytesRef);
        fieldType.indexedToReadable(bytesRef, charsRef);
        String group = charsRef.toString();
        outMap.put(group, slice);
      }
    }

    rb.rsp.add("expanded", outMap);
  }
        @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {

  }
        @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {

    if(!rb.doExpand) {
      return;
    }

    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      SolrQueryRequest req = rb.req;
      Map expanded = (Map)req.getContext().get("expanded");
      if(expanded == null) {
        expanded = new HashMap();
        req.getContext().put("expanded", expanded);
      }

      for (ShardResponse srsp : sreq.responses) {
        NamedList response = srsp.getSolrResponse().getResponse();
        Map ex = (Map)response.get("expanded");
        Iterator<Map.Entry<String,SolrDocumentList>>it = ex.entrySet().iterator();
        while(it.hasNext()) {
          Map.Entry<String, SolrDocumentList> entry = it.next();
          String name = entry.getKey();
          SolrDocumentList val = entry.getValue();
          expanded.put(name, val);
        }
      }
    }
  }
        @Override
  public void finishStage(ResponseBuilder rb) {

    if(!rb.doExpand) {
      return;
    }

    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return;
    }

    Map expanded = (Map)rb.req.getContext().get("expanded");
    if(expanded == null) {
      expanded = new HashMap();
    }

    rb.rsp.add("expanded", expanded);
  }

  private class GroupExpandCollector extends Collector {
    private SortedDocValues docValues;
    private IntObjectOpenHashMap groups;
    private int docBase;
    private FixedBitSet groupBits;
    private IntOpenHashSet collapsedSet;
    private List<Collector> collectors;

    public GroupExpandCollector(SortedDocValues docValues, FixedBitSet groupBits, IntOpenHashSet collapsedSet, int limit, Sort sort) throws IOException {
      int numGroups = collapsedSet.size();
      groups = new IntObjectOpenHashMap(numGroups*2);
      collectors = new ArrayList();
      DocIdSetIterator iterator = groupBits.iterator();
      int group = -1;
      while((group = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        Collector collector = (sort == null) ? TopScoreDocCollector.create(limit, true) : TopFieldCollector.create(sort,limit, false, false,false, true);
        groups.put(group, collector);
        collectors.add(collector);
      }

      this.collapsedSet = collapsedSet;
      this.groupBits = groupBits;
      this.docValues = docValues;
    }

    public IntObjectOpenHashMap getGroups() {
      return this.groups;
    }

    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    public void collect(int docId) throws IOException {
      int doc = docId+docBase;
      int ord = docValues.getOrd(doc);
      if(ord > -1 && groupBits.get(ord) && !collapsedSet.contains(doc)) {
        Collector c = (Collector)groups.get(ord);
        c.collect(docId);
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.docBase = context.docBase;
      for(Collector c : collectors) {
        c.setNextReader(context);
      }
    }

    public void setScorer(Scorer scorer) throws IOException {
      for(Collector c : collectors) {
        c.setScorer(scorer);
      }
    }
  }

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

    @Override
  public String getDescription() {
    return "Expand Component";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/core/src/java/org/apache/solr/handler/component/ExpandComponent.java $";
  }

  @Override
  public URL[] getDocs() {
    try {
      return new URL[]{
          new URL("http://wiki.apache.org/solr/ExpandComponent")
      };
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }
}