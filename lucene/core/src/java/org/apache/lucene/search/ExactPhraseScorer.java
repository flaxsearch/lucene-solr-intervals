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

import org.apache.lucene.search.PhraseQuery.TermDocsEnumFactory;
import org.apache.lucene.search.intervals.BlockIntervalIterator;
import org.apache.lucene.search.intervals.IntervalIterator;
import org.apache.lucene.search.intervals.TermIntervalIterator;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

final class ExactPhraseScorer extends Scorer {
  private final int endMinus1;
  
  private final static int CHUNK = 4096;
  
  private int gen;
  private final int[] counts = new int[CHUNK];
  private final int[] gens = new int[CHUNK];
  
  boolean noDocs;

  private final long cost;

  private final static class ChunkState {

    final PostingsEnum posEnum;
    final int offset;
    int posUpto;
    int posLimit;
    int pos;
    int lastPos;

    public ChunkState(PostingsEnum posEnum, int offset) {
      this.posEnum = posEnum;
      this.offset = offset;
    }
  }

  private final ConjunctionDISI conjunction;

  private final ChunkState[] chunkStates;
  private final PostingsEnum lead;

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

    chunkStates = new ChunkState[postings.length];

    endMinus1 = postings.length-1;
    
    lead = postings[0].postings;
    // min(cost)
    cost = lead.cost();

    List<DocIdSetIterator> iterators = new ArrayList<>();
    for(int i=0;i<postings.length;i++) {
      chunkStates[i] = new ChunkState(postings[i].postings, -postings[i].position);
      iterators.add(postings[i].postings);
    }
    conjunction = ConjunctionDISI.intersect(iterators);
  }

  @Override
  public TwoPhaseDocIdSetIterator asTwoPhaseIterator() {
    return new TwoPhaseDocIdSetIterator() {

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

  @Override
  public int docID() {
    return conjunction.docID();
  }
  
  @Override
  public float score() {
    return docScorer.score(docID(), freq);
  }
  
  private int phraseFreq() throws IOException {
    
    freq = 0;
    
    // init chunks
    for (int i = 0; i < chunkStates.length; i++) {
      final ChunkState cs = chunkStates[i];
      cs.posLimit = cs.posEnum.freq();
      cs.pos = cs.offset + cs.posEnum.nextPosition();
      cs.posUpto = 1;
      cs.lastPos = -1;
    }
    
    int chunkStart = 0;
    int chunkEnd = CHUNK;
    
    // process chunk by chunk
    boolean end = false;
    
    // TODO: we could fold in chunkStart into offset and
    // save one subtract per pos incr
    
    while (!end) {
      
      gen++;
      
      if (gen == 0) {
        // wraparound
        Arrays.fill(gens, 0);
        gen++;
      }
      
      // first term
      {
        final ChunkState cs = chunkStates[0];
        while (cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            counts[posIndex] = 1;
            assert gens[posIndex] != gen;
            gens[posIndex] = gen;
          }
          
          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }
      
      // middle terms
      boolean any = true;
      for (int t = 1; t < endMinus1; t++) {
        final ChunkState cs = chunkStates[t];
        any = false;
        while (cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            if (posIndex >= 0 && gens[posIndex] == gen && counts[posIndex] == t) {
              // viable
              counts[posIndex]++;
              any = true;
            }
          }
          
          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
        
        if (!any) {
          break;
        }
      }
      
      if (!any) {
        // petered out for this chunk
        chunkStart += CHUNK;
        chunkEnd += CHUNK;
        continue;
      }
      
      // last term
      
      {
        final ChunkState cs = chunkStates[endMinus1];
        while (cs.pos < chunkEnd) {
          if (cs.pos > cs.lastPos) {
            cs.lastPos = cs.pos;
            final int posIndex = cs.pos - chunkStart;
            if (posIndex >= 0 && gens[posIndex] == gen
                && counts[posIndex] == endMinus1) {
              freq++;
              if (!needsScores) {
                return freq; // we determined there was a match.
              }
            }
          }
          
          if (cs.posUpto == cs.posLimit) {
            end = true;
            break;
          }
          cs.posUpto++;
          cs.pos = cs.offset + cs.posEnum.nextPosition();
        }
      }
      
      chunkStart += CHUNK;
      chunkEnd += CHUNK;
    }
    
    return freq;
  }

  @Override
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    TermIntervalIterator[] posIters = new TermIntervalIterator[chunkStates.length];
    PostingsEnum[] enums = new PostingsEnum[chunkStates.length];
    for (int i = 0; i < chunkStates.length; i++) {
      posIters[i] = new TermIntervalIterator(this, enums[i] = chunkStates[i].posEnum,
                                              false, collectIntervals, field);
    }
    return new SloppyPhraseScorer.AdvancingIntervalIterator(this, collectIntervals, enums, new BlockIntervalIterator(this, collectIntervals, posIters));
  }

  @Override
  public long cost() {
    return cost;
  }
}
