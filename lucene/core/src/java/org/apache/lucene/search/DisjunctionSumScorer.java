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

import org.apache.lucene.search.ScorerPriorityQueue.ScorerWrapper;
import org.apache.lucene.search.intervals.DisjunctionIntervalIterator;
import org.apache.lucene.search.intervals.IntervalIterator;

import java.io.IOException;
import java.util.List;

/** A Scorer for OR like queries, counterpart of <code>ConjunctionScorer</code>.
 * This Scorer implements {@link Scorer#advance(int)} and uses advance() on the given Scorers. 
 */
final class DisjunctionSumScorer extends DisjunctionScorer { 
  private final float[] coord;
  
  /** Construct a <code>DisjunctionScorer</code>.
   * @param weight The weight to be used.
   * @param subScorers Array of at least two subscorers.
   * @param coord Table of coordination factors
   */
  DisjunctionSumScorer(Weight weight, List<Scorer> subScorers, float[] coord, boolean needsScores) {
    super(weight, subScorers, needsScores);
    this.coord = coord;
  }

  @Override
  protected float score(ScorerWrapper topList) throws IOException {
    double score = 0;
    int freq = 0;
    for (ScorerWrapper w = topList; w != null; w = w.next) {
      score += w.scorer.score();
      freq += 1;
    }
    return (float)score * coord[freq];
  }
  
  @Override
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    return new DisjunctionIntervalIterator(this, collectIntervals, pullIterators(collectIntervals, getSubScorers()));
  }

}
