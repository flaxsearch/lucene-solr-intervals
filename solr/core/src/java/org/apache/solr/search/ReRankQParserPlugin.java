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

package org.apache.solr.search;

import com.carrotsearch.hppc.IntIntOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.QueryRescorer;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrQueryRequest;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;

import org.apache.lucene.search.ScoreDoc;
import com.carrotsearch.hppc.IntFloatOpenHashMap;

import org.apache.lucene.util.Bits;
import org.apache.solr.request.SolrRequestInfo;

import java.io.IOException;
import java.util.Map;
import java.util.Arrays;
import java.util.Comparator;

/*
*
*  Syntax: q=*:*&rq={!rerank reRankQuery=$rqq reRankDocs=300 reRankWeight=3}
*
*/

public class ReRankQParserPlugin extends QParserPlugin {

  public static final String NAME = "rerank";
  private static Query defaultQuery = new MatchAllDocsQuery();

  public void init(NamedList args) {
  }

  public QParser createParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new ReRankQParser(query, localParams, params, req);
  }

  private class ReRankQParser extends QParser  {

    public ReRankQParser(String query, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(query, localParams, params, req);
    }

    public Query parse() throws SyntaxError {

      String reRankQueryString = localParams.get("reRankQuery");
      QParser reRankParser = QParser.getParser(reRankQueryString, null, req);
      Query reRankQuery = reRankParser.parse();

      int reRankDocs  = localParams.getInt("reRankDocs", 200);
      double reRankWeight = localParams.getDouble("reRankWeight",2.0d);

      int start = params.getInt(CommonParams.START,0);
      int rows = params.getInt(CommonParams.ROWS,10);

      // This enusres that reRankDocs >= docs needed to satisfy the result set.
      reRankDocs = Math.max(start+rows, reRankDocs);

      return new ReRankQuery(reRankQuery, reRankDocs, reRankWeight);
    }
  }

  private class ReRankQuery extends RankQuery {
    private Query mainQuery = defaultQuery;
    private Query reRankQuery;
    private int reRankDocs;
    private double reRankWeight;
    private Map<BytesRef, Integer> boostedPriority;

    public int hashCode() {
      return mainQuery.hashCode()+reRankQuery.hashCode()+(int)reRankWeight+reRankDocs+(int)getBoost();
    }

    public boolean equals(Object o) {
      if(o instanceof ReRankQuery) {
        ReRankQuery rrq = (ReRankQuery)o;
        return (mainQuery.equals(rrq.mainQuery) &&
                reRankQuery.equals(rrq.reRankQuery) &&
                reRankWeight == rrq.reRankWeight &&
                reRankDocs == rrq.reRankDocs &&
                getBoost() == rrq.getBoost());
      }
      return false;
    }

    public ReRankQuery(Query reRankQuery, int reRankDocs, double reRankWeight) {
      this.reRankQuery = reRankQuery;
      this.reRankDocs = reRankDocs;
      this.reRankWeight = reRankWeight;
    }

    public RankQuery wrap(Query _mainQuery) {
      if(_mainQuery != null){
        this.mainQuery = _mainQuery;
      }
      return  this;
    }

    public MergeStrategy getMergeStrategy() {
      return null;
    }

    public TopDocsCollector getTopDocsCollector(int len, SolrIndexSearcher.QueryCommand cmd, IndexSearcher searcher) throws IOException {

      if(this.boostedPriority == null) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if(info != null) {
          Map context = info.getReq().getContext();
          this.boostedPriority = (Map<BytesRef, Integer>)context.get(QueryElevationComponent.BOOSTED_PRIORITY);
        }
      }

      return new ReRankCollector(reRankDocs, reRankQuery, reRankWeight, cmd, searcher, boostedPriority);
    }

    public String toString(String s) {
      return "{!rerank mainQuery='"+mainQuery.toString()+
             "' reRankQuery='"+reRankQuery.toString()+
             "' reRankDocs="+reRankDocs+
             " reRankWeigh="+reRankWeight+"}";
    }

    public String toString() {
      return toString(null);
    }

    public Weight createWeight(IndexSearcher searcher) throws IOException{
      return new ReRankWeight(mainQuery, reRankQuery, reRankWeight, searcher);
    }
  }

  private class ReRankWeight extends Weight{
    private Query reRankQuery;
    private IndexSearcher searcher;
    private Weight mainWeight;
    private double reRankWeight;

    public ReRankWeight(Query mainQuery, Query reRankQuery, double reRankWeight, IndexSearcher searcher) throws IOException {
      this.reRankQuery = reRankQuery;
      this.searcher = searcher;
      this.reRankWeight = reRankWeight;
      this.mainWeight = mainQuery.createWeight(searcher);
    }

    public float getValueForNormalization() throws IOException {
      return mainWeight.getValueForNormalization();
    }

    public Scorer scorer(AtomicReaderContext context, PostingFeatures features, Bits bits) throws IOException {
      return mainWeight.scorer(context, features, bits);
    }

    public Query getQuery() {
      return mainWeight.getQuery();
    }

    public void normalize(float norm, float topLevelBoost) {
      mainWeight.normalize(norm, topLevelBoost);
    }

    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Explanation mainExplain = mainWeight.explain(context, doc);
      return new QueryRescorer(reRankQuery) {
        @Override
        protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
          float score = firstPassScore;
          if (secondPassMatches) {
            score += reRankWeight * secondPassScore;
          }
          return score;
        }
      }.explain(searcher, mainExplain, context.docBase+doc);
    }
  }

  private class ReRankCollector extends TopDocsCollector {

    private Query reRankQuery;
    private TopDocsCollector  mainCollector;
    private IndexSearcher searcher;
    private int reRankDocs;
    private double reRankWeight;
    private Map<BytesRef, Integer> boostedPriority;

    public ReRankCollector(int reRankDocs,
                           Query reRankQuery,
                           double reRankWeight,
                           SolrIndexSearcher.QueryCommand cmd,
                           IndexSearcher searcher,
                           Map<BytesRef, Integer> boostedPriority) throws IOException {
      super(null);
      this.reRankQuery = reRankQuery;
      this.reRankDocs = reRankDocs;
      this.boostedPriority = boostedPriority;
      Sort sort = cmd.getSort();
      if(sort == null) {
        this.mainCollector = TopScoreDocCollector.create(this.reRankDocs,true);
      } else {
        sort = sort.rewrite(searcher);
        this.mainCollector = TopFieldCollector.create(sort, this.reRankDocs, false, true, true, true);
      }
      this.searcher = searcher;
      this.reRankWeight = reRankWeight;
    }

    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    public void collect(int doc) throws IOException {
      mainCollector.collect(doc);
    }

    public void setScorer(Scorer scorer) throws IOException{
      mainCollector.setScorer(scorer);
    }

    public void doSetNextReader(AtomicReaderContext context) throws IOException{
      mainCollector.getLeafCollector(context);
    }

    public int getTotalHits() {
      return mainCollector.getTotalHits();
    }

    public TopDocs topDocs(int start, int howMany) {
      try {
        TopDocs mainDocs = mainCollector.topDocs(0, reRankDocs);

        if(boostedPriority != null) {
          SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
          Map requestContext = null;
          if(info != null) {
            requestContext = info.getReq().getContext();
          }

          IntIntOpenHashMap boostedDocs = QueryElevationComponent.getBoostDocs((SolrIndexSearcher)searcher, boostedPriority, requestContext);

          TopDocs rescoredDocs = new QueryRescorer(reRankQuery) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
              float score = firstPassScore;
              if (secondPassMatches) {
                score += reRankWeight * secondPassScore;
              }
              return score;
            }
          }.rescore(searcher, mainDocs, reRankDocs);

          Arrays.sort(rescoredDocs.scoreDocs, new BoostedComp(boostedDocs, mainDocs.scoreDocs, rescoredDocs.getMaxScore()));

          if(howMany > rescoredDocs.scoreDocs.length) {
            howMany = rescoredDocs.scoreDocs.length;
          }

          ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
          System.arraycopy(rescoredDocs.scoreDocs,0,scoreDocs,0,howMany);
          rescoredDocs.scoreDocs = scoreDocs;
          return rescoredDocs;
        } else {
          return new QueryRescorer(reRankQuery) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
              float score = firstPassScore;
              if (secondPassMatches) {
                score += reRankWeight * secondPassScore;
              }
              return score;
            }
          }.rescore(searcher, mainDocs, howMany);
        }

      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
      }
    }
  }

  public class BoostedComp implements Comparator {
    IntFloatOpenHashMap boostedMap;

    public BoostedComp(IntIntOpenHashMap boostedDocs, ScoreDoc[] scoreDocs, float maxScore) {
      this.boostedMap = new IntFloatOpenHashMap(boostedDocs.size()*2);

      for(int i=0; i<scoreDocs.length; i++) {
        if(boostedDocs.containsKey(scoreDocs[i].doc)) {
          boostedMap.put(scoreDocs[i].doc, maxScore+boostedDocs.lget());
        } else {
          break;
        }
      }
    }

    public int compare(Object o1, Object o2) {
      ScoreDoc doc1 = (ScoreDoc) o1;
      ScoreDoc doc2 = (ScoreDoc) o2;
      float score1 = doc1.score;
      float score2 = doc2.score;
      if(boostedMap.containsKey(doc1.doc)) {
        score1 = boostedMap.lget();
      }

      if(boostedMap.containsKey(doc2.doc)) {
        score2 = boostedMap.lget();
      }

      if(score1 > score2) {
        return -1;
      } else if(score1 < score2) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}