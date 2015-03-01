package org.apache.lucene.spatial.prefix;

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

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;

/**
 * Base class for Lucene Filters on SpatialPrefixTree fields.
 * @lucene.experimental
 */
public abstract class AbstractPrefixTreeFilter extends Filter {

  protected final Shape queryShape;
  protected final String fieldName;
  protected final SpatialPrefixTree grid;//not in equals/hashCode since it's implied for a specific field
  protected final int detailLevel;

  public AbstractPrefixTreeFilter(Shape queryShape, String fieldName, SpatialPrefixTree grid, int detailLevel) {
    this.queryShape = queryShape;
    this.fieldName = fieldName;
    this.grid = grid;
    this.detailLevel = detailLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!getClass().equals(o.getClass())) return false;

    AbstractPrefixTreeFilter that = (AbstractPrefixTreeFilter) o;

    if (detailLevel != that.detailLevel) return false;
    if (!fieldName.equals(that.fieldName)) return false;
    if (!queryShape.equals(that.queryShape)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = queryShape.hashCode();
    result = 31 * result + fieldName.hashCode();
    result = 31 * result + detailLevel;
    return result;
  }

  /** Holds transient state and docid collecting utility methods as part of
   * traversing a {@link TermsEnum} for a {@link org.apache.lucene.index.LeafReaderContext}. */
  public abstract class BaseTermsEnumTraverser {//TODO rename to LeafTermsEnumTraverser ?
    //note: only 'fieldName' (accessed in constructor) keeps this from being a static inner class

    protected final LeafReaderContext context;
    protected Bits acceptDocs;
    protected final int maxDoc;

    protected TermsEnum termsEnum;//remember to check for null!
    protected PostingsEnum postingsEnum;

    public BaseTermsEnumTraverser(LeafReaderContext context, Bits acceptDocs) throws IOException {
      this.context = context;
      LeafReader reader = context.reader();
      this.acceptDocs = acceptDocs;
      this.maxDoc = reader.maxDoc();
      Terms terms = reader.terms(fieldName);
      if (terms != null)
        this.termsEnum = terms.iterator(null);
    }

    protected void collectDocs(BitSet bitSet) throws IOException {
      assert termsEnum != null;
      postingsEnum = termsEnum.postings(acceptDocs, postingsEnum, PostingsEnum.NONE);
      bitSet.or(postingsEnum);
    }

  }

}
