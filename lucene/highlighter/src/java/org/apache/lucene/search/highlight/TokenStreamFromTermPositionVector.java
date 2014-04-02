package org.apache.lucene.search.highlight;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;

/**
 * TokenStream created from a term vector field.
 */
public final class TokenStreamFromTermPositionVector extends TokenStream {

  private final List<Token> positionedTokens = new ArrayList<>();

  private Iterator<Token> tokensAtCurrentPosition;

  private CharTermAttribute termAttribute;

  private PositionIncrementAttribute positionIncrementAttribute;

  private OffsetAttribute offsetAttribute;

  private PayloadAttribute payloadAttribute;

  /**
   * Constructor.
   * 
   * @param vector Terms that contains the data for
   *        creating the TokenStream. Must have positions and offsets.
   */
  public TokenStreamFromTermPositionVector(
      final Terms vector) throws IOException {
    termAttribute = addAttribute(CharTermAttribute.class);
    positionIncrementAttribute = addAttribute(PositionIncrementAttribute.class);
    offsetAttribute = addAttribute(OffsetAttribute.class);
    payloadAttribute = addAttribute(PayloadAttribute.class);
    final boolean hasOffsets = vector.hasOffsets();
    final boolean hasPayloads = vector.hasPayloads();
    final TermsEnum termsEnum = vector.iterator(null);
    BytesRef text;
    DocsAndPositionsEnum dpEnum = null;
    while((text = termsEnum.next()) != null) {
      dpEnum = termsEnum.docsAndPositions(null, dpEnum);
      assert dpEnum != null; // presumably checked by TokenSources.hasPositions earlier
      dpEnum.nextDoc();
      final int freq = dpEnum.freq();
      for (int j = 0; j < freq; j++) {
        int pos = dpEnum.nextPosition();
        Token token;
        if (hasOffsets) {
          token = new Token(text.utf8ToString(),
                            dpEnum.startOffset(),
                            dpEnum.endOffset());
        } else {
          token = new Token();
          token.setEmpty().append(text.utf8ToString());
        }
        if (hasPayloads) {
          // Must make a deep copy of the returned payload,
          // since D&PEnum API is allowed to re-use on every
          // call:
          token.setPayload(BytesRef.deepCopyOf(dpEnum.getPayload()));
        }

        // Yes - this is the position, not the increment! This is for
        // sorting. This value
        // will be corrected before use.
        token.setPositionIncrement(pos);
        this.positionedTokens.add(token);
      }
    }
    CollectionUtil.timSort(this.positionedTokens, tokenComparator);
    int lastPosition = -1;
    for (final Token token : this.positionedTokens) {
      int thisPosition = token.getPositionIncrement();
      token.setPositionIncrement(thisPosition - lastPosition);
      lastPosition = thisPosition;
    }
    this.tokensAtCurrentPosition = this.positionedTokens.iterator();
  }

  private static final Comparator<Token> tokenComparator = new Comparator<Token>() {
    @Override
    public int compare(final Token o1, final Token o2) {
      return o1.getPositionIncrement() - o2.getPositionIncrement();
    }
  };
  
  @Override
  public boolean incrementToken() {
    if (this.tokensAtCurrentPosition.hasNext()) {
      final Token next = this.tokensAtCurrentPosition.next();
      clearAttributes();
      termAttribute.setEmpty().append(next);
      positionIncrementAttribute.setPositionIncrement(next
          .getPositionIncrement());
      offsetAttribute.setOffset(next.startOffset(), next.endOffset());
      payloadAttribute.setPayload(next.getPayload());
      return true;
    }
    return false;
  }

  @Override
  public void reset() {
    this.tokensAtCurrentPosition = this.positionedTokens.iterator();
  }
}
