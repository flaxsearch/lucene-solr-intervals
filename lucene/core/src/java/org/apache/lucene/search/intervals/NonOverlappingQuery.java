package org.apache.lucene.search.intervals;

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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Set;

/**
 * A Query that matches documents containing an interval (the minuend) that
 * does not contain another interval (the subtrahend).
 *
 * As an example, given the following {@link org.apache.lucene.search.BooleanQuery}:
 * <pre>
 *   BooleanQuery bq = new BooleanQuery();
 *   bq.add(new TermQuery(new Term(field, "quick")), BooleanQuery.Occur.MUST);
 *   bq.add(new TermQuery(new Term(field, "fox")), BooleanQuery.Occur.MUST);
 * </pre>
 *
 * The document "the quick brown fox" will be matched by this query.  But
 * create a NonOverlappingQuery using this query as a minuend:
 * <pre>
 *   NonOverlappingQuery brq = new NonOverlappingQuery(bq, new TermQuery(new Term(field, "brown")));
 * </pre>
 *
 * This query will not match "the quick brown fox", because "brown" is found
 * within the interval of the boolean query for "quick" and "fox.  The query
 * will match "the quick fox is brown", because here "brown" is outside
 * the minuend's interval.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the Brouwerian operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 * @see BrouwerianIntervalIterator
 */
public final class NonOverlappingQuery extends Query implements Cloneable {
  
  private final Query minuend;
  private final Query subtrahend;
  private final String field;

  /**
   * Constructs a Query that matches documents containing intervals of the minuend
   * that are not subtended by the subtrahend
   * @param minuend the minuend Query
   * @param subtrahend the subtrahend Query
   */
  public NonOverlappingQuery(Query minuend, Query subtrahend) {
    this.minuend = minuend;
    this.subtrahend = subtrahend;
    if (minuend.getFields().size() != 1)
      throw new IllegalArgumentException("Minuend query must be defined on a single field: found " + minuend.getFields());
    if (subtrahend.getFields().size() != 1)
      throw new IllegalArgumentException("Subtrahend query must be defined on a single field: found " + subtrahend.getFields());
    if (!minuend.getFields().containsAll(subtrahend.getFields()))
      throw new IllegalArgumentException("Minuend and subtrahend must be on the same field: found "
                                          + minuend.getFields() + " and " + subtrahend.getFields());
    field = minuend.getFields().iterator().next();
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    minuend.extractTerms(terms);
    subtrahend.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten =  minuend.rewrite(reader);
    Query subRewritten =  subtrahend.rewrite(reader);
    if (rewritten != minuend || subRewritten != subtrahend) {
      return new NonOverlappingQuery(rewritten, subRewritten);
    }
    return this;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, int flags) throws IOException {
    return new BrouwerianQueryWeight(minuend.createWeight(searcher, needsScores, flags),
                                     subtrahend.createWeight(searcher, needsScores, flags));
  }

  class BrouwerianQueryWeight extends Weight {

    private final Weight minuted;
    private final Weight subtracted;

    public BrouwerianQueryWeight(Weight minuted, Weight subtracted) {
      super(NonOverlappingQuery.this);
      this.minuted = minuted;
      this.subtracted = subtracted;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc)
        throws IOException {
      return minuted.explain(context, doc);
    }

    @Override
    public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
      ScorerFactory factory = new ScorerFactory(minuted, subtracted, context, acceptDocs);
      final Scorer scorer = factory.minutedScorer();
      final Scorer subScorer = factory.subtractedScorer();
      if (subScorer == null) {
        return scorer;
      }
      return scorer == null ? null : new BrouwerianScorer(this, scorer, subScorer, factory);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return minuted.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      minuted.normalize(norm, topLevelBoost);
    }
  }
  
  static class ScorerFactory {
    final Weight minuted;
    final Weight subtracted;
    final LeafReaderContext context;
    final Bits acceptDocs;
    ScorerFactory(Weight minuted, Weight subtracted,
                  LeafReaderContext context, Bits acceptDocs) {
      this.minuted = minuted;
      this.subtracted = subtracted;
      this.context = context;
      this.acceptDocs = acceptDocs;
    }
    
    public Scorer minutedScorer() throws IOException {
      return minuted.scorer(context, acceptDocs);
    }
    
    public Scorer subtractedScorer() throws IOException {
      return subtracted.scorer(context, acceptDocs);
    }
    
  }
  
  final class BrouwerianScorer extends Scorer {

    private final Scorer minuend;
    private IntervalIterator filter;
    private final Scorer subtracted;
    Interval current;
    private final ScorerFactory factory;

    public BrouwerianScorer(Weight weight, Scorer minuend, Scorer subtracted, ScorerFactory factory) throws IOException {
      super(weight);
      this.minuend = minuend;
      this.subtracted = subtracted;
      this.filter = new BrouwerianIntervalIterator(minuend, false, minuend.intervals(false),
                                                    subtracted.intervals(false), field);
      this.factory = factory;
    }

    @Override
    public float score() throws IOException {
      return minuend.score();
    }

    @Override
    public IntervalIterator intervals(boolean collectIntervals) throws IOException {
      if (collectIntervals) {
        final Scorer minuted  = factory.minutedScorer();
        final Scorer subtracted = factory.subtractedScorer();
        final BrouwerianIntervalIterator brouwerianIntervalIterator
            = new BrouwerianIntervalIterator(subtracted, true, minuted.intervals(true), subtracted.intervals(true), field);
        return new IntervalIterator(this, collectIntervals) {

          @Override
          public int scorerAdvanced(int docId) throws IOException {
            //if (docId == this.docID())
            //  return docId;
            int mId = minuted.advance(docId);
            if (subtracted.docID() < docId)
              subtracted.advance(docId);
            if (mId <= docId)
              return brouwerianIntervalIterator.scorerAdvanced(docId);
            return mId;
          }

          @Override
          public Interval next() throws IOException {
            return brouwerianIntervalIterator.next();
          }

          @Override
          public void collect(IntervalCollector collector) {
            brouwerianIntervalIterator.collect(collector);
          }

          @Override
          public IntervalIterator[] subs(boolean inOrder) {
            return brouwerianIntervalIterator.subs(inOrder);
          }

          @Override
          public int matchDistance() {
            return brouwerianIntervalIterator.matchDistance();
          }
          
        };
      }
      

      
      return new IntervalIterator(this, false) {
        private boolean buffered = true;
        @Override
        public int scorerAdvanced(int docId) throws IOException {
          buffered = true;
          assert docId == filter.docID();
          return docId;
        }

        @Override
        public Interval next() throws IOException {
          if (buffered) {
            buffered = false;
            return current;
          }
          else if (current != null) {
            return current = filter.next();
          }
          return null;
        }

        @Override
        public void collect(IntervalCollector collector) {
          filter.collect(collector);
        }

        @Override
        public IntervalIterator[] subs(boolean inOrder) {
          return filter.subs(inOrder);
        }

        @Override
        public int matchDistance() {
          return filter.matchDistance();
        }
        
      };
    }

    @Override
    public int docID() {
      return minuend.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = -1;
      while ((docId = minuend.nextDoc()) != Scorer.NO_MORE_DOCS) {
        if (subtracted.docID() < docId)
          subtracted.advance(docId);
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) { // just check if there is a position that matches!
          return minuend.docID();
        }
      }
      return Scorer.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      int docId = minuend.advance(target);
      subtracted.advance(docId);
      if (docId == Scorer.NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      do {
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) {
          return minuend.docID();
        }
      } while ((docId = minuend.nextDoc()) != Scorer.NO_MORE_DOCS);
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return minuend.cost();
    }

    @Override
    public int freq() throws IOException {
      return minuend.freq();
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

  }

  @Override
  public String toString(String field) {
    return "NonOverlappingQuery[" + minuend + ", " + subtrahend + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((minuend == null) ? 0 : minuend.hashCode());
    result = prime * result
        + ((subtrahend == null) ? 0 : subtrahend.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    NonOverlappingQuery other = (NonOverlappingQuery) obj;
    if (minuend == null) {
      if (other.minuend != null) return false;
    } else if (!minuend.equals(other.minuend)) return false;
    if (subtrahend == null) {
      if (other.subtrahend != null) return false;
    } else if (!subtrahend.equals(other.subtrahend)) return false;
    return true;
  }

}