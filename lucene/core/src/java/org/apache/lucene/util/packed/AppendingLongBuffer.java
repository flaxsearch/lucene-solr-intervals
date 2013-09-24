package org.apache.lucene.util.packed;

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


/**
 * Utility class to buffer a list of signed longs in memory. This class only
 * supports appending and is optimized for the case where values are close to
 * each other.
 * @lucene.internal
 */
public final class AppendingLongBuffer extends AbstractAppendingLongBuffer {

  /** @param initialPageCount the initial number of pages
   *  @param pageSize         the size of a single page */
  public AppendingLongBuffer(int initialPageCount, int pageSize) {
    super(initialPageCount, pageSize);
  }

  /** Create an {@link AppendingLongBuffer} with initialPageCount=16 and
   *  pageSize=1024. */
  public AppendingLongBuffer() {
    this(16, 1024);
  }

  @Override
  long get(int block, int element) {
    if (block == valuesOff) {
      return pending[element];
    } else if (deltas[block] == null) {
      return minValues[block];
    } else {
      return minValues[block] + deltas[block].get(element);
    }
  }

  @Override
  void packPendingValues() {
    // compute max delta
    long minValue = pending[0];
    long maxValue = pending[0];
    for (int i = 1; i < pendingOff; ++i) {
      minValue = Math.min(minValue, pending[i]);
      maxValue = Math.max(maxValue, pending[i]);
    }
    final long delta = maxValue - minValue;

    minValues[valuesOff] = minValue;
    if (delta == 0) {
      deltas[valuesOff] = new PackedInts.NullReader(pendingOff);
    } else {
      // build a new packed reader
      final int bitsRequired = delta < 0 ? 64 : PackedInts.bitsRequired(delta);
      for (int i = 0; i < pendingOff; ++i) {
        pending[i] -= minValue;
      }
      final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired, PackedInts.COMPACT);
      for (int i = 0; i < pendingOff; ) {
        i += mutable.set(i, pending, i, pendingOff - i);
      }
      deltas[valuesOff] = mutable;
    }
  }

  /** Return an iterator over the values of this buffer. */
  @Override
  public Iterator iterator() {
    return new Iterator();
  }

  /** A long iterator. */
  public final class Iterator extends AbstractAppendingLongBuffer.Iterator {

    Iterator() {
      super();
    }

    @Override
    void fillValues() {
      if (vOff == valuesOff) {
        currentValues = pending;
        currentCount = pendingOff;
      } else {
        currentCount = deltas[vOff].size();
        for (int k = 0; k < currentCount; ) {
          k += deltas[vOff].get(k, currentValues, k, currentCount - k);
        }
        for (int k = 0; k < currentCount; ++k) {
          currentValues[k] += minValues[vOff];
        }
      }
    }

  }

}
