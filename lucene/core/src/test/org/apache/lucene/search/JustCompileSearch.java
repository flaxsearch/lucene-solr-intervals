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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/**
 * Holds all implementations of classes in the o.a.l.search package as a
 * back-compatibility test. It does not run any tests per-se, however if 
 * someone adds a method to an interface or abstract method to an abstract
 * class, one of the implementations here will fail to compile and so we know
 * back-compat policy was violated.
 */
final class JustCompileSearch {

  private static final String UNSUPPORTED_MSG = "unsupported: used for back-compat testing only !";

  static final class JustCompileCollector extends Collector {

    @Override
    public void collect(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setScorer(Scorer scorer) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

  }
  
  static final class JustCompileDocIdSet extends DocIdSet {

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileDocIdSetIterator extends DocIdSetIterator {

    @Override
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }
  
  static final class JustCompileExtendedFieldCacheLongParser implements FieldCache.LongParser {

    @Override
    public long parseLong(BytesRef string) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public TermsEnum termsEnum(Terms terms) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
  static final class JustCompileExtendedFieldCacheDoubleParser implements FieldCache.DoubleParser {
    
    @Override
    public double parseDouble(BytesRef term) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public TermsEnum termsEnum(Terms terms) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFieldComparator extends FieldComparator<Object> {

    @Override
    public int compare(int slot1, int slot2) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int compareBottom(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void copy(int slot, int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setBottom(int slot) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public FieldComparator<Object> setNextReader(AtomicReaderContext context) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Object value(int slot) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int compareDocToValue(int doc, Object value) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileFieldComparatorSource extends FieldComparatorSource {

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits,
        int sortPos, boolean reversed) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFilter extends Filter {
    // Filter is just an abstract class with no abstract methods. However it is
    // still added here in case someone will add abstract methods in the future.
    
    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
      return null;
    }
  }

  static final class JustCompileFilteredDocIdSet extends FilteredDocIdSet {

    public JustCompileFilteredDocIdSet(DocIdSet innerSet) {
      super(innerSet);
    }

    @Override
    protected boolean match(int docid) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }

  static final class JustCompileFilteredDocIdSetIterator extends FilteredDocIdSetIterator {

    public JustCompileFilteredDocIdSetIterator(DocIdSetIterator innerIter) {
      super(innerIter);
    }

    @Override
    protected boolean match(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileQuery extends Query {

    @Override
    public String toString(String field) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
  static final class JustCompileScorer extends Scorer {

    protected JustCompileScorer(Weight weight) {
      super(weight);
    }

    @Override
    public boolean score(Collector collector, int max, int firstDocID) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public float score() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public int freq() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public IntervalIterator intervals(boolean collectIntervals) throws IOException {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }
  
  static final class JustCompileSimilarity extends Similarity {

    @Override
    public SimWeight computeWeight(float queryBoost, CollectionStatistics collectionStats, TermStatistics... termStats) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public SimScorer simScorer(SimWeight stats, AtomicReaderContext context) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public long computeNorm(FieldInvertState state) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
  }

  static final class JustCompileTopDocsCollector extends TopDocsCollector<ScoreDoc> {

    protected JustCompileTopDocsCollector(PriorityQueue<ScoreDoc> pq) {
      super(pq);
    }

    @Override
    public void collect(int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void setScorer(Scorer scorer) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public TopDocs topDocs() {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public TopDocs topDocs( int start ) {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }

    @Override
    public TopDocs topDocs( int start, int end ) {
        throw new UnsupportedOperationException( UNSUPPORTED_MSG );
    }
    
  }

  static final class JustCompileWeight extends Weight {

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Query getQuery() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public float getValueForNormalization() {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, PostingFeatures flags, Bits acceptDocs) {
      throw new UnsupportedOperationException(UNSUPPORTED_MSG);
    }
    
  }
  
}
