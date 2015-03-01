package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RoaringDocIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Wraps another {@link Query}'s result and caches it when scores are not
 * needed.  The purpose is to allow queries to simply care about matching and
 * scoring, and then wrap with this class to add caching.
 */
public class CachingWrapperQuery extends Query implements Accountable {
  private Query query; // not final because of clone
  private final QueryCachingPolicy policy;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another query's result and caches it according to the provided policy.
   * @param query Query to cache results of
   * @param policy policy defining which filters should be cached on which segments
   */
  public CachingWrapperQuery(Query query, QueryCachingPolicy policy) {
    this.query = query;
    this.policy = policy;
  }

  /** Same as {@link CachingWrapperQuery#CachingWrapperQuery(Query, QueryCachingPolicy)}
   *  but enforces the use of the
   *  {@link QueryCachingPolicy.CacheOnLargeSegments#DEFAULT} policy. */
  public CachingWrapperQuery(Query query) {
    this(query, QueryCachingPolicy.CacheOnLargeSegments.DEFAULT);
  }

  @Override
  public CachingWrapperQuery clone() {
    final CachingWrapperQuery clone = (CachingWrapperQuery) super.clone();
    clone.query = query.clone();
    return clone;
  }

  /**
   * Gets the contained query.
   * @return the contained query.
   */
  public Query getQuery() {
    return query;
  }
  
  @Override
  public float getBoost() {
    return query.getBoost();
  }
  
  @Override
  public void setBoost(float b) {
    query.setBoost(b);
  }
  
  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);
    if (query == rewritten) {
      return this;
    } else {
      CachingWrapperQuery clone = clone();
      clone.query = rewritten;
      return clone;
    }
  }

  // for testing
  int hitCount, missCount;

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
    final Weight weight = query.createWeight(searcher, needsScores, flags);
    if (needsScores) {
      // our cache is not sufficient, we need scores too
      return weight;
    }
    policy.onUse(weight.getQuery());
    return new ConstantScoreWeight(weight.getQuery()) {
      @Override
      Scorer scorer(LeafReaderContext context, final Bits acceptDocs, float score) throws IOException {
        final LeafReader reader = context.reader();
        final Object key = reader.getCoreCacheKey();

        DocIdSet docIdSet = cache.get(key);
        if (docIdSet != null) {
          hitCount++;
        } else if (policy.shouldCache(query, context)) {
          missCount++;
          final Scorer scorer = weight.scorer(context, null);
          if (scorer == null) {
            docIdSet = DocIdSet.EMPTY;
          } else {
            docIdSet = cacheImpl(scorer, context.reader());
          }
          cache.put(key, docIdSet);
        } else {
          return weight.scorer(context, acceptDocs);
        }

        assert docIdSet != null;
        if (docIdSet == DocIdSet.EMPTY) {
          return null;
        }
        final DocIdSetIterator approximation = docIdSet.iterator();
        if (approximation == null) {
          return null;
        }

        final DocIdSetIterator disi;
        final TwoPhaseIterator twoPhaseView;
        if (acceptDocs == null) {
          twoPhaseView = null;
          disi = approximation;
        } else {
          twoPhaseView = new TwoPhaseIterator() {
            
            @Override
            public boolean matches() throws IOException {
              final int doc = approximation.docID();
              return acceptDocs.get(doc);
            }
            
            @Override
            public DocIdSetIterator approximation() {
              return approximation;
            }
          };
          disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseView);
        }
        return new Scorer(weight) {

          @Override
          public TwoPhaseIterator asTwoPhaseIterator() {
            return twoPhaseView;
          }

          @Override
          public IntervalIterator intervals(boolean collectIntervals) throws IOException {
            return null;
          }

          @Override
          public float score() throws IOException {
            return 0f;
          }

          @Override
          public int freq() throws IOException {
            return 1;
          }

          @Override
          public int docID() {
            return disi.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return disi.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return disi.advance(target);
          }

          @Override
          public long cost() {
            return disi.cost();
          }
          
        };
      }
    };
  }
  
  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "("+query.toString(field)+")";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !getClass().equals(o.getClass())) return false;
    final CachingWrapperQuery other = (CachingWrapperQuery) o;
    return this.query.equals(other.query);
  }

  @Override
  public int hashCode() {
    return (query.hashCode() ^ getClass().hashCode());
  }

  @Override
  public long ramBytesUsed() {

    // Sync only to pull the current set of values:
    List<DocIdSet> docIdSets;
    synchronized(cache) {
      docIdSets = new ArrayList<>(cache.values());
    }

    long total = 0;
    for(DocIdSet dis : docIdSets) {
      total += dis.ramBytesUsed();
    }

    return total;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    // Sync to pull the current set of values:
    synchronized (cache) {
      // no need to clone, Accountable#namedAccountables already copies the data
      return Accountables.namedAccountables("segment", cache);
    }
  }
}
