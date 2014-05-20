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

public class RangeFrequencyFilter implements IntervalFilter {

  private final int maxFreq;
  private final int minFreq;

  public RangeFrequencyFilter(int minFreq, int maxFreq) {
    if (minFreq < 0)
      throw new IllegalArgumentException("minFreq must be greater than 0");
    if (maxFreq < minFreq)
      throw new IllegalArgumentException("maxFreq must be greater than minFreq");
    this.maxFreq = maxFreq;
    this.minFreq = minFreq;
  }

  @Override
  public IntervalIterator filter(boolean collectIntervals, IntervalIterator iter) {
    return new MaxFrequencyIntervalIterator(minFreq, maxFreq, iter, collectIntervals);
  }

  public static class MaxFrequencyIntervalIterator extends IntervalIterator {

    private final IntervalIterator subIter;
    private final Interval[] intervalCache;
    private final int[] distanceCache;
    private final int minFreq;

    private int cachePos = -1;
    private int freq = -1;

    public MaxFrequencyIntervalIterator(int minFreq, int maxFreq, IntervalIterator iter, boolean collectIntervals) {
      super(iter == null ? null : iter.scorer, collectIntervals);
      this.minFreq = minFreq;
      this.subIter = iter;
      this.intervalCache = new Interval[maxFreq];
      for (int i = 0; i < maxFreq; i++) {
        this.intervalCache[i] = new Interval();
      }
      this.distanceCache = new int[maxFreq];
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
      if (freq == -1 || freq < minFreq)
        return null;
      cachePos++;
      if (cachePos < freq)
        return intervalCache[cachePos];
      return null;
    }

    private int loadIntervalCache() throws IOException {
      int f = 0;
      Interval interval;
      while ((interval = subIter.next()) != null) {
        if (f >= intervalCache.length)
          return -1;
        intervalCache[f].copy(interval);
        f++;
      }
      return f;
    }

    @Override
    public void collect(IntervalCollector collector) {
      collector.collectComposite(null, intervalCache[cachePos], subIter.docID());
    }

    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return new IntervalIterator[]{ subIter };
    }

    @Override
    public int matchDistance() {
      return distanceCache[cachePos];
    }
  }

  @Override
  public String toString() {
    return "RANGEFREQ(" + minFreq + "," + maxFreq + ")";
  }
}
