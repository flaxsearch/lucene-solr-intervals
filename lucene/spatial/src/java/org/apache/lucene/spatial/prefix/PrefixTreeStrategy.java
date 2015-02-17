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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.ShapeFieldCacheDistanceValueSource;

/**
 * An abstract SpatialStrategy based on {@link SpatialPrefixTree}. The two
 * subclasses are {@link RecursivePrefixTreeStrategy} and {@link
 * TermQueryPrefixTreeStrategy}.  This strategy is most effective as a fast
 * approximate spatial search filter.
 * <p>
 * <b>Characteristics:</b>
 * <br>
 * <ul>
 * <li>Can index any shape; however only {@link RecursivePrefixTreeStrategy}
 * can effectively search non-point shapes.</li>
 * <li>Can index a variable number of shapes per field value. This strategy
 * can do it via multiple calls to {@link #createIndexableFields(com.spatial4j.core.shape.Shape)}
 * for a document or by giving it some sort of Shape aggregate (e.g. JTS
 * WKT MultiPoint).  The shape's boundary is approximated to a grid precision.
 * </li>
 * <li>Can query with any shape.  The shape's boundary is approximated to a grid
 * precision.</li>
 * <li>Only {@link org.apache.lucene.spatial.query.SpatialOperation#Intersects}
 * is supported.  If only points are indexed then this is effectively equivalent
 * to IsWithin.</li>
 * <li>The strategy supports {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point,double)}
 * even for multi-valued data, so long as the indexed data is all points; the
 * behavior is undefined otherwise.  However, <em>it will likely be removed in
 * the future</em> in lieu of using another strategy with a more scalable
 * implementation.  Use of this call is the only
 * circumstance in which a cache is used.  The cache is simple but as such
 * it doesn't scale to large numbers of points nor is it real-time-search
 * friendly.</li>
 * </ul>
 * <p>
 * <b>Implementation:</b>
 * <p>
 * The {@link SpatialPrefixTree} does most of the work, for example returning
 * a list of terms representing grids of various sizes for a supplied shape.
 * An important
 * configuration item is {@link #setDistErrPct(double)} which balances
 * shape precision against scalability.  See those javadocs.
 *
 * @lucene.internal
 */
public abstract class PrefixTreeStrategy extends SpatialStrategy {
  protected final SpatialPrefixTree grid;
  private final Map<String, PointPrefixTreeFieldCacheProvider> provider = new ConcurrentHashMap<>();
  protected int defaultFieldValuesArrayLen = 2;
  protected double distErrPct = SpatialArgs.DEFAULT_DISTERRPCT;// [ 0 TO 0.5 ]
  protected boolean pointsOnly = false;//if true, there are no leaves

  public PrefixTreeStrategy(SpatialPrefixTree grid, String fieldName) {
    super(grid.getSpatialContext(), fieldName);
    this.grid = grid;
  }

  public SpatialPrefixTree getGrid() {
    return grid;
  }

  /**
   * A memory hint used by {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}
   * for how big the initial size of each Document's array should be. The
   * default is 2.  Set this to slightly more than the default expected number
   * of points per document.
   */
  public void setDefaultFieldValuesArrayLen(int defaultFieldValuesArrayLen) {
    this.defaultFieldValuesArrayLen = defaultFieldValuesArrayLen;
  }

  public double getDistErrPct() {
    return distErrPct;
  }

  /**
   * The default measure of shape precision affecting shapes at index and query
   * times. Points don't use this as they are always indexed at the configured
   * maximum precision ({@link org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree#getMaxLevels()});
   * this applies to all other shapes. Specific shapes at index and query time
   * can use something different than this default value.  If you don't set a
   * default then the default is {@link SpatialArgs#DEFAULT_DISTERRPCT} --
   * 2.5%.
   *
   * @see org.apache.lucene.spatial.query.SpatialArgs#getDistErrPct()
   */
  public void setDistErrPct(double distErrPct) {
    this.distErrPct = distErrPct;
  }

  public boolean isPointsOnly() {
    return pointsOnly;
  }

  /** True if only indexed points shall be supported. There are no "leafs" in such a case.  See
   *  {@link org.apache.lucene.spatial.prefix.IntersectsPrefixTreeFilter#hasIndexedLeaves}. */
  public void setPointsOnly(boolean pointsOnly) {
    this.pointsOnly = pointsOnly;
  }

  @Override
  public Field[] createIndexableFields(Shape shape) {
    double distErr = SpatialArgs.calcDistanceFromErrPct(shape, distErrPct, ctx);
    return createIndexableFields(shape, distErr);
  }

  /**
   * Turns {@link SpatialPrefixTree#getTreeCellIterator(Shape, int)} into a
   * {@link org.apache.lucene.analysis.TokenStream}.
   * {@code simplifyIndexedCells} is an optional hint affecting non-point shapes: it will
   * simply/aggregate sets of complete leaves in a cell to its parent, resulting in ~20-25%
   * fewer cells. It will likely be removed in the future.
   */
  public Field[] createIndexableFields(Shape shape, double distErr) {
    int detailLevel = grid.getLevelForDistance(distErr);
    TokenStream tokenStream = createTokenStream(shape, detailLevel);
    Field field = new Field(getFieldName(), tokenStream, FIELD_TYPE);
    return new Field[]{field};
  }

  protected TokenStream createTokenStream(Shape shape, int detailLevel) {
    if (pointsOnly && !(shape instanceof Point)) {
      throw new IllegalArgumentException("pointsOnly is true yet a " + shape.getClass() + " is given for indexing");
    }
    Iterator<Cell> cells = grid.getTreeCellIterator(shape, detailLevel);
    return new CellTokenStream().setCells(cells);
  }

  /* Indexed, tokenized, not stored. */
  public static final FieldType FIELD_TYPE = new FieldType();

  static {
    FIELD_TYPE.setTokenized(true);
    FIELD_TYPE.setOmitNorms(true);
    FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
    FIELD_TYPE.freeze();
  }

  @Override
  public ValueSource makeDistanceValueSource(Point queryPoint, double multiplier) {
    PointPrefixTreeFieldCacheProvider p = provider.get( getFieldName() );
    if( p == null ) {
      synchronized (this) {//double checked locking idiom is okay since provider is threadsafe
        p = provider.get( getFieldName() );
        if (p == null) {
          p = new PointPrefixTreeFieldCacheProvider(grid, getFieldName(), defaultFieldValuesArrayLen);
          provider.put(getFieldName(),p);
        }
      }
    }

    return new ShapeFieldCacheDistanceValueSource(ctx, p, queryPoint, multiplier);
  }

  /**
   * Computes spatial facets in two dimensions as a grid of numbers.  The data is often visualized as a so-called
   * "heatmap".
   *
   * @see org.apache.lucene.spatial.prefix.HeatmapFacetCounter#calcFacets(PrefixTreeStrategy, org.apache.lucene.index.IndexReaderContext, org.apache.lucene.search.Filter, com.spatial4j.core.shape.Shape, int, int)
   */
  public HeatmapFacetCounter.Heatmap calcFacets(IndexReaderContext context, Filter filter,
                                   Shape inputShape, final int facetLevel, int maxCells) throws IOException {
    return HeatmapFacetCounter.calcFacets(this, context, filter, inputShape, facetLevel, maxCells);
  }
}
