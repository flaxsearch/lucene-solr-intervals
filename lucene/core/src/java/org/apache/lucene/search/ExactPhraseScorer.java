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

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.intervals.BlockIntervalIterator;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.TermIntervalIterator;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class ExactPhraseScorer extends Scorer {

  private static class PostingsAndPosition {
    private final PostingsEnum postings;
    private final int offset;
    private int freq, upTo, pos;

    public PostingsAndPosition(PostingsEnum postings, int offset) {
      this.postings = postings;
      this.offset = offset;
    }
  }

  private final ConjunctionDISI conjunction;
  private final PostingsAndPosition[] postings;

  private int freq;

  private final Similarity.SimScorer docScorer;

  private final boolean needsScores;

  private final String field;
  
  ExactPhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
                    Similarity.SimScorer docScorer, boolean needsScores, String field) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.needsScores = needsScores;
    this.field = field;

    List<DocIdSetIterator> iterators = new ArrayList<>();
    List<PostingsAndPosition> postingsAndPositions = new ArrayList<>();
    for(PhraseQuery.PostingsAndFreq posting : postings) {
      iterators.add(posting.postings);
      postingsAndPositions.add(new PostingsAndPosition(posting.postings, posting.position));
    }
    conjunction = ConjunctionDISI.intersect(iterators);
    this.postings = postingsAndPositions.toArray(new PostingsAndPosition[postingsAndPositions.size()]);
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    return new TwoPhaseIterator() {

      @Override
      public boolean matches() throws IOException {
        return phraseFreq() > 0;
      }

      @Override
      public DocIdSetIterator approximation() {
        return conjunction;
      }
    };
  }

  private int doNext(int doc) throws IOException {
    for (;; doc = conjunction.nextDoc()) {
      if (doc == NO_MORE_DOCS || phraseFreq() > 0) {
        return doc;
      }
    }
  }
  
  @Override
  public int nextDoc() throws IOException {
    return doNext(conjunction.nextDoc());
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(conjunction.advance(target));
  }
  
  @Override
  public String toString() {
    return "ExactPhraseScorer(" + weight + ")";
  }
  
  @Override
  public int freq() {
    return freq;
  }
  
  @Override
  public int docID() {
    return conjunction.docID();
  }
  
  @Override
  public float score() {
    return docScorer.score(docID(), freq);
  }

  /** Advance the given pos enum to the first doc on or after {@code target}.
   *  Return {@code false} if the enum was exhausted before reaching
   *  {@code target} and {@code true} otherwise. */
  private static boolean advancePosition(PostingsAndPosition posting, int target) throws IOException {
    while (posting.pos < target) {
      if (posting.upTo == posting.freq) {
        return false;
      } else {
        posting.pos = posting.postings.nextPosition();
        posting.upTo += 1;
      }
    }
    return true;
  }

  private int phraseFreq() throws IOException {
    // reset state
    final PostingsAndPosition[] postings = this.postings;
    for (PostingsAndPosition posting : postings) {
      posting.freq = posting.postings.freq();
      posting.pos = posting.postings.nextPosition();
      posting.upTo = 1;
    }

    int freq = 0;
    final PostingsAndPosition lead = postings[0];

    advanceHead:
    while (true) {
      final int phrasePos = lead.pos - lead.offset;
      for (int j = 1; j < postings.length; ++j) {
        final PostingsAndPosition posting = postings[j];
        final int expectedPos = phrasePos + posting.offset;

        // advance up to the same position as the lead
        if (advancePosition(posting, expectedPos) == false) {
          break advanceHead;
        }

        if (posting.pos != expectedPos) { // we advanced too far
          if (advancePosition(lead, posting.pos - posting.offset + lead.offset)) {
            continue advanceHead;
          } else {
            break advanceHead;
          }
        }
      }

      freq += 1;
      if (needsScores == false) {
        break;
      }

      if (lead.upTo == lead.freq) {
        break;
      }
      lead.pos = lead.postings.nextPosition();
      lead.upTo += 1;
    }

    return this.freq = freq;
  }

  @Override
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    TermIntervalIterator[] posIters = new TermIntervalIterator[postings.length];
    PostingsEnum[] enums = new PostingsEnum[postings.length];
    for (int i = 0; i < postings.length; i++) {
      posIters[i] = new TermIntervalIterator(this, enums[i] = postings[i].postings,
                                              false, collectIntervals, field);
    }
    return new SloppyPhraseScorer.AdvancingIntervalIterator(this, collectIntervals, enums, new BlockIntervalIterator(this, collectIntervals, posIters));
  }

  @Override
  public long cost() {
    return conjunction.cost();
  }
}
