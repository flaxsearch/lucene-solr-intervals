package org.apache.lucene.search.intervals;

import java.io.IOException;

/**
 * Copyright (c) 2014 Lemur Consulting Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class MinFrequencyFilter implements IntervalFilter {

  private final int minFreq;

  public MinFrequencyFilter(int minFreq) {
    this.minFreq = minFreq;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return new MinFrequencyIntervalIterator(minFreq, iter, collectIntervals);
  }

  public static class MinFrequencyIntervalIterator extends IntervalIterator {

    private final IntervalIterator subIter;
    private final Interval[] intervalCache;
    private final int[] distanceCache;

    private int cachePos = -1;
    private int freq = -1;

    public MinFrequencyIntervalIterator(int minFreq, IntervalIterator iter, boolean collectIntervals) {
      super(iter == null ? null : iter.scorer, collectIntervals);
      this.subIter = iter;
      this.intervalCache = new Interval[minFreq];
      for (int i = 0; i < minFreq; i++) {
        this.intervalCache[i] = new Interval();
      }
      this.distanceCache = new int[minFreq];
    }

    @Override
    public int scorerAdvanced(int docId) throws IOException {
      cachePos = -1;
      return subIter.scorerAdvanced(docId);
    }

    @Override
    public Interval next() throws IOException {
      if (cachePos == -1)
        freq = loadIntervalCache();
      if (freq < intervalCache.length)
        return null;
      cachePos++;
      if (cachePos < intervalCache.length)
        return intervalCache[cachePos];
      return subIter.next();
    }

    private int loadIntervalCache() throws IOException {
      int f = 0;
      Interval interval;
      while (f < intervalCache.length && (interval = subIter.next()) != null) {
        intervalCache[f].copy(interval);
        f++;
      }
      return f;
    }

    @Override
    public void collect(IntervalCollector collector) {
      if (cachePos < distanceCache.length)
        collector.collectComposite(null, intervalCache[cachePos], subIter.docID());
      subIter.collect(collector);
    }

    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return new IntervalIterator[]{ subIter };
    }

    @Override
    public int matchDistance() {
      if (cachePos < distanceCache.length)
        return distanceCache[cachePos];
      return subIter.matchDistance();
    }
  }

  @Override
  public String toString() {
    return "MINFREQ(" + minFreq + ")";
  }
}
