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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Weight.PostingFeatures;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * A Query that filters the results of an inner {@link Query} using an
 * {@link IntervalFilter}.
 *
 * @see OrderedNearQuery
 * @see UnorderedNearQuery
 * @see NonOverlappingQuery
 *
 * @lucene.experimental
 */
public class IntervalFilterQuery extends FieldedQuery implements Cloneable {

  private Query inner;
  private final IntervalFilter filter;

  /**
   * Constructs a query using an inner query and an IntervalFilter
   * @param inner the query to wrap
   * @param filter the filter to restrict results by
   */
  public IntervalFilterQuery(FieldedQuery inner, IntervalFilter filter) {
    super(inner.getField());
    this.inner = inner;
    this.filter = filter;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    inner.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    IntervalFilterQuery clone = null;

    Query rewritten =  inner.rewrite(reader);
    if (rewritten != inner) {
      clone = (IntervalFilterQuery) this.clone();
      clone.inner = rewritten;
    }

    if (clone != null) {
      return clone; // some clauses rewrote
    } else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new IntervalFilterWeight(inner.createWeight(searcher), searcher);
  }

  class IntervalFilterWeight extends Weight {

    private final Weight other;
    private final Similarity similarity;
    private final Similarity.SimWeight stats;

    public IntervalFilterWeight(Weight other, IndexSearcher searcher) throws IOException {
      this.other = other;
      this.similarity = searcher.getSimilarity();
      this.stats = getSimWeight(other.getQuery(), searcher);
    }

    private Similarity.SimWeight getSimWeight(Query query, IndexSearcher searcher)  throws IOException {
      TreeSet<Term> terms = new TreeSet<Term>();
      query.extractTerms(terms);
      if (terms.size() == 0)
        return null;
      int i = 0;
      TermStatistics[] termStats = new TermStatistics[terms.size()];
      for (Term term : terms) {
        TermContext state = TermContext.build(searcher.getTopReaderContext(), term);
        termStats[i] = searcher.termStatistics(term, state);
        i++;
      }
      final String field = terms.first().field(); // nocommit - should we be checking all filtered terms
                                                  // are on the same field?
      return similarity.computeWeight(query.getBoost(), searcher.collectionStatistics(field), termStats);

    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc)
        throws IOException {
      Scorer scorer = scorer(context, PostingFeatures.POSITIONS,
                              context.reader().getLiveDocs());
      if (scorer != null) {
        int newDoc = scorer.advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          Similarity.SimScorer docScorer = similarity.simScorer(stats, context);
          ComplexExplanation result = new ComplexExplanation();
          result.setDescription("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "], result of:");
          Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "phraseFreq=" + freq));
          result.addDetail(scoreExplanation);
          result.setValue(scoreExplanation.getValue());
          result.setMatch(true);
          return result;
        }
      }
      return new ComplexExplanation(false, 0.0f,
          "No matching term within position filter");
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, PostingFeatures flags, Bits acceptDocs) throws IOException {
      if (stats == null)
        return null;
      flags = flags == PostingFeatures.DOCS_AND_FREQS ? PostingFeatures.POSITIONS : flags;
      ScorerFactory factory = new ScorerFactory(other, context, flags, acceptDocs);
      final Scorer scorer = factory.scorer();
      Similarity.SimScorer docScorer = similarity.simScorer(stats, context);
      return scorer == null ? null : new IntervalFilterScorer(this, scorer, factory, docScorer);
    }

    @Override
    public Query getQuery() {
      return IntervalFilterQuery.this;
    }
    
    @Override
    public float getValueForNormalization() throws IOException {
      return stats == null ? 1.0f : stats.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      if (stats != null)
        stats.normalize(norm, topLevelBoost);
    }
  }
  
  static class ScorerFactory {
    final Weight weight;
    final AtomicReaderContext context;
    final PostingFeatures flags;
    final Bits acceptDocs;
    ScorerFactory(Weight weight,
        AtomicReaderContext context, PostingFeatures flags,
        Bits acceptDocs) {
      this.weight = weight;
      this.context = context;
      this.flags = flags;
      this.acceptDocs = acceptDocs;
    }
    
    public Scorer scorer() throws IOException {
      return weight.scorer(context, flags, acceptDocs);
    }
    
  }

  final class IntervalFilterScorer extends Scorer {

    private final Scorer other;
    private IntervalIterator filter;
    private Interval current;
    private final ScorerFactory factory;
    private final Similarity.SimScorer docScorer;

    public IntervalFilterScorer(Weight weight, Scorer other, ScorerFactory factory,
                                Similarity.SimScorer docScorer) throws IOException {
      super(weight);
      this.other = other;
      this.factory = factory;
      this.filter = IntervalFilterQuery.this.filter.filter(false, other.intervals(false));
      this.docScorer = docScorer;
    }

    @Override
    public float score() throws IOException {
      return docScorer.score(docID(), freq());
    }

    @Override
    public IntervalIterator intervals(boolean collectIntervals) throws IOException {
      if (collectIntervals) {
        final Scorer collectingScorer = factory.scorer();
        final IntervalIterator filter = IntervalFilterQuery.this.filter.filter(true, collectingScorer.intervals(true));
        return new IntervalIterator(this, true) {

          @Override
          public int scorerAdvanced(int docId) throws IOException {
            //System.out.println("IntervalIterator: advancing from " + collectingScorer.docID() + " to " + docId);
            if (collectingScorer.docID() >= docId) {
              return collectingScorer.docID();
            }
            int target = collectingScorer.advance(docId);
            if (target == NO_MORE_DOCS)
              return NO_MORE_DOCS;
            return filter.scorerAdvanced(target);
          }

          @Override
          public Interval next() throws IOException {
            return filter.next();
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

          @Override
          public int docID() {
            return filter.docID();
          }

          @Override
          public String toString() {
            return IntervalFilterQuery.this.toString(null) + "[" + filter + "]";
          }
          
        };
      }
      
      return new IntervalIterator(this, collectIntervals) {
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
      return other.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docId = -1;
      while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS) {
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) { // just check if there is at least one interval that matches!
          return other.docID();
        }
      }
      return Scorer.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      int docId = other.advance(target);
      if (docId == Scorer.NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      do {
        filter.scorerAdvanced(docId);
        if ((current = filter.next()) != null) {
          return other.docID();
        }
      } while ((docId = other.nextDoc()) != Scorer.NO_MORE_DOCS);
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return other.cost();
    }

    @Override
    public int freq() throws IOException {
      return 1; // nocommit how to calculate frequency?
    }

    public float sloppyFreq() throws IOException {
      float freq = 0.0f;
      do {
        int d = filter.matchDistance();
        freq += docScorer.computeSlopFactor(d);
      }
      while (filter.next() != null);
      return freq;
    }

  }

  @Override
  public String toString(String field) {
    return "Filtered(" + inner.toString() + ")";
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((filter == null) ? 0 : filter.hashCode());
    result = prime * result + ((inner == null) ? 0 : inner.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    IntervalFilterQuery other = (IntervalFilterQuery) obj;
    if (filter == null) {
      if (other.filter != null) return false;
    } else if (!filter.equals(other.filter)) return false;
    if (inner == null) {
      if (other.inner != null) return false;
    } else if (!inner.equals(other.inner)) return false;
    return true;
  }

}