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

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

abstract class ConstantScoreWeight extends Weight {

  private float queryNorm;
  private float queryWeight;

  protected ConstantScoreWeight(Query query) {
    super(query);
  }

  @Override
  public final float getValueForNormalization() throws IOException {
    queryWeight = getQuery().getBoost();
    return queryWeight * queryWeight;
  }

  @Override
  public final void normalize(float norm, float topLevelBoost) {
    queryNorm = norm * topLevelBoost;
    queryWeight *= queryNorm;
  }

  @Override
  public final Explanation explain(LeafReaderContext context, int doc) throws IOException {
    final Scorer s = scorer(context, context.reader().getLiveDocs());
    final boolean exists = (s != null && s.advance(doc) == doc);

    final ComplexExplanation result = new ComplexExplanation();
    if (exists) {
      result.setDescription(getQuery().toString() + ", product of:");
      result.setValue(queryWeight);
      result.setMatch(Boolean.TRUE);
      result.addDetail(new Explanation(getQuery().getBoost(), "boost"));
      result.addDetail(new Explanation(queryNorm, "queryNorm"));
    } else {
      result.setDescription(getQuery().toString() + " doesn't match id " + doc);
      result.setValue(0);
      result.setMatch(Boolean.FALSE);
    }
    return result;
  }

  @Override
  public final Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
    return scorer(context, acceptDocs, queryWeight);
  }

  abstract Scorer scorer(LeafReaderContext context, Bits acceptDocs, float score) throws IOException;

}
