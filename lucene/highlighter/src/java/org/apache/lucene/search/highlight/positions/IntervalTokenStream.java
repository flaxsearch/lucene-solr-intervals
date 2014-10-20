package org.apache.lucene.search.highlight.positions;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.search.intervals.Interval;
import org.apache.lucene.search.intervals.IntervalIterator;

/**
 * A TokenStream constructed from a stream of positions and their offsets.
 * The document is segmented into tokens at the start and end offset of each interval.  The intervals
 * are assumed to be non-overlapping.
 * 
 * TODO: abstract the dependency on the current PositionOffsetMapper impl; 
 * allow for implementations of position->offset maps that don't rely on term vectors.
 * 
 * @lucene.experimental
 */
public class IntervalTokenStream extends TokenStream {

  //this tokenizer generates four attributes:
  // term, offset, positionIncrement? and type?
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  //private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final String text;
  private final IntervalIterator positions;
  
  // the index of the current position interval
  private Interval pos = null;
  
  public IntervalTokenStream (String text, IntervalIterator positions) {
    this.text = text;
    this.positions = positions;
  }
  
  @Override
  public final boolean incrementToken() throws IOException {
    pos = positions.next();
    if (pos == null){
      return false;
    }
    int b, e; 
    b = pos.offsetBegin;
    e = pos.offsetEnd;
    assert b >=0;
    termAtt.append(text, b, e);
    offsetAtt.setOffset(b, e);
    posIncrAtt.setPositionIncrement(1);
    return true;
  }

}
