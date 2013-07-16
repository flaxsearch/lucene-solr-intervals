package org.apache.lucene.analysis.miscellaneous;

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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Removes words that are too long or too short from the stream.
 * <p>
 * Note: Length is calculated as the number of UTF-16 code units.
 * </p>
 */
public final class LengthFilter extends FilteringTokenFilter {

  private final int min;
  private final int max;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Create a new {@link LengthFilter}. This will filter out tokens whose
   * {@link CharTermAttribute} is either too short ({@link CharTermAttribute#length()}
   * &lt; min) or too long ({@link CharTermAttribute#length()} &gt; max).
   * @param version the Lucene match version
   * @param in      the {@link TokenStream} to consume
   * @param min     the minimum length
   * @param max     the maximum length
   */
  public LengthFilter(Version version, TokenStream in, int min, int max) {
    super(version, in);
    this.min = min;
    this.max = max;
  }

  @Override
  public boolean accept() {
    final int len = termAtt.length();
    return (len >= min && len <= max);
  }
}
