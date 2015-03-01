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

import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SentinelIntSet;

import java.io.IOException;
import java.util.Arrays;

/**
 * Finds docs where its indexed shape {@link org.apache.lucene.spatial.query.SpatialOperation#Contains
 * CONTAINS} the query shape. For use on {@link RecursivePrefixTreeStrategy}.
 *
 * @lucene.experimental
 */
public class ContainsPrefixTreeFilter extends AbstractPrefixTreeFilter {

  /*
  Future optimizations:
    Instead of seekExact, use seekCeil with some leap-frogging, like Intersects does.
  */

  /**
   * If the spatial data for a document is comprised of multiple overlapping or adjacent parts,
   * it might fail to match a query shape when doing the CONTAINS predicate when the sum of
   * those shapes contain the query shape but none do individually.  Set this to false to
   * increase performance if you don't care about that circumstance (such as if your indexed
   * data doesn't even have such conditions).  See LUCENE-5062.
   */
  protected final boolean multiOverlappingIndexedShapes;

  public ContainsPrefixTreeFilter(Shape queryShape, String fieldName, SpatialPrefixTree grid, int detailLevel, boolean multiOverlappingIndexedShapes) {
    super(queryShape, fieldName, grid, detailLevel);
    this.multiOverlappingIndexedShapes = multiOverlappingIndexedShapes;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;
    return multiOverlappingIndexedShapes == ((ContainsPrefixTreeFilter)o).multiOverlappingIndexedShapes;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + (multiOverlappingIndexedShapes ? 1 : 0);
  }

  @Override
  public String toString(String field) {
    return "ContainsPrefixTreeFilter(" +
        // TODO: print something about the shape?
        "fieldName=" + fieldName + "," +
        "detailLevel=" + detailLevel + "," +
        "multiOverlappingIndexedShapes=" + multiOverlappingIndexedShapes +
        ")";
  }

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
    return new ContainsVisitor(context, acceptDocs).visit(grid.getWorldCell(), acceptDocs);
  }

  private class ContainsVisitor extends BaseTermsEnumTraverser {

    public ContainsVisitor(LeafReaderContext context, Bits acceptDocs) throws IOException {
      super(context, acceptDocs);
    }

    //The reused value of cell.getTokenBytesNoLeaf which is always then seek()'ed to. It's used in assertions too.
    BytesRef termBytes = new BytesRef();//no leaf
    Cell nextCell;//see getLeafDocs

    /** This is the primary algorithm; recursive.  Returns null if finds none. */
    private SmallDocSet visit(Cell cell, Bits acceptContains) throws IOException {

      if (termsEnum == null)//signals all done
        return null;

      // Leaf docs match all query shape
      SmallDocSet leafDocs = getLeafDocs(cell, acceptContains);

      // Get the AND of all child results (into combinedSubResults)
      SmallDocSet combinedSubResults = null;
      //   Optimization: use null subCellsFilter when we know cell is within the query shape.
      Shape subCellsFilter = queryShape;
      if (cell.getLevel() != 0 && ((cell.getShapeRel() == null || cell.getShapeRel() == SpatialRelation.WITHIN))) {
        subCellsFilter = null;
        assert cell.getShape().relate(queryShape) == SpatialRelation.WITHIN;
      }
      CellIterator subCells = cell.getNextLevelCells(subCellsFilter);
      while (subCells.hasNext()) {
        Cell subCell = subCells.next();
        if (!seekExact(subCell))
          combinedSubResults = null;
        else if (subCell.getLevel() == detailLevel)
          combinedSubResults = getDocs(subCell, acceptContains);
        else if (!multiOverlappingIndexedShapes &&
            subCell.getShapeRel() == SpatialRelation.WITHIN)
          combinedSubResults = getLeafDocs(subCell, acceptContains);
        else
          combinedSubResults = visit(subCell, acceptContains); //recursion

        if (combinedSubResults == null)
          break;
        acceptContains = combinedSubResults;//has the 'AND' effect on next iteration
      }

      // Result: OR the leaf docs with AND of all child results
      if (combinedSubResults != null) {
        if (leafDocs == null)
          return combinedSubResults;
        return leafDocs.union(combinedSubResults);//union is 'or'
      }
      return leafDocs;
    }

    private boolean seekExact(Cell cell) throws IOException {
      assert cell.getTokenBytesNoLeaf(null).compareTo(termBytes) > 0;
      if (termsEnum == null)
        return false;
      termBytes = cell.getTokenBytesNoLeaf(termBytes);
      assert assertCloneTermBytes(); //assertions look at termBytes later on
      return termsEnum.seekExact(termBytes);
    }

    private boolean assertCloneTermBytes() {
      termBytes = BytesRef.deepCopyOf(termBytes);
      return true;
    }

    private SmallDocSet getDocs(Cell cell, Bits acceptContains) throws IOException {
      assert cell.getTokenBytesNoLeaf(null).equals(termBytes);

      return collectDocs(acceptContains);
    }

    /** Gets docs on the leaf of the given cell, _if_ there is a leaf cell, otherwise null. */
    private SmallDocSet getLeafDocs(Cell cell, Bits acceptContains) throws IOException {
      assert cell.getTokenBytesNoLeaf(null).equals(termBytes);

      if (termsEnum == null)
        return null;
      BytesRef nextTerm = termsEnum.next();
      if (nextTerm == null) {
        termsEnum = null;//signals all done
        return null;
      }
      nextCell = grid.readCell(nextTerm, nextCell);
      assert cell.isPrefixOf(nextCell);
      if (nextCell.getLevel() == cell.getLevel() && nextCell.isLeaf()) {
        return collectDocs(acceptContains);
      } else {
        return null;
      }
    }

    private SmallDocSet collectDocs(Bits acceptContains) throws IOException {
      SmallDocSet set = null;

      postingsEnum = termsEnum.postings(acceptContains, postingsEnum, PostingsEnum.NONE);
      int docid;
      while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (set == null) {
          int size = termsEnum.docFreq();
          if (size <= 0)
            size = 16;
          set = new SmallDocSet(size);
        }
        set.set(docid);
      }
      return set;
    }

  }//class ContainsVisitor

  /** A hash based mutable set of docIds. If this were Solr code then we might
   * use a combination of HashDocSet and SortedIntDocSet instead. */
  private static class SmallDocSet extends DocIdSet implements Bits {

    private final SentinelIntSet intSet;
    private int maxInt = 0;

    public SmallDocSet(int size) {
      intSet = new SentinelIntSet(size, -1);
    }

    @Override
    public boolean get(int index) {
      return intSet.exists(index);
    }

    public void set(int index) {
      intSet.put(index);
      if (index > maxInt)
        maxInt = index;
    }

    /** Largest docid. */
    @Override
    public int length() {
      return maxInt;
    }

    /** Number of docids. */
    public int size() {
      return intSet.size();
    }

    /** NOTE: modifies and returns either "this" or "other" */
    public SmallDocSet union(SmallDocSet other) {
      SmallDocSet bigger;
      SmallDocSet smaller;
      if (other.intSet.size() > this.intSet.size()) {
        bigger = other;
        smaller = this;
      } else {
        bigger = this;
        smaller = other;
      }
      //modify bigger
      for (int v : smaller.intSet.keys) {
        if (v == smaller.intSet.emptyVal)
          continue;
        bigger.set(v);
      }
      return bigger;
    }

    @Override
    public Bits bits() throws IOException {
      //if the # of docids is super small, return null since iteration is going
      // to be faster
      return size() > 4 ? this : null;
    }

    @Override
    public DocIdSetIterator iterator() throws IOException {
      if (size() == 0)
        return null;
      //copy the unsorted values to a new array then sort them
      int d = 0;
      final int[] docs = new int[intSet.size()];
      for (int v : intSet.keys) {
        if (v == intSet.emptyVal)
          continue;
        docs[d++] = v;
      }
      assert d == intSet.size();
      final int size = d;

      //sort them
      Arrays.sort(docs, 0, size);

      return new DocIdSetIterator() {
        int idx = -1;
        @Override
        public int docID() {
          if (idx >= 0 && idx < size)
            return docs[idx];
          else
            return -1;
        }

        @Override
        public int nextDoc() throws IOException {
          if (++idx < size)
            return docs[idx];
          return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
          //for this small set this is likely faster vs. a binary search
          // into the sorted array
          return slowAdvance(target);
        }

        @Override
        public long cost() {
          return size;
        }
      };
    }

    @Override
    public long ramBytesUsed() {
      return RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_OBJECT_REF
          + RamUsageEstimator.NUM_BYTES_INT)
          + intSet.ramBytesUsed();
    }

  }//class SmallDocSet

}
