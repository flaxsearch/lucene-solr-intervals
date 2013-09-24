package org.apache.lucene.spatial;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;

/**
 * This class serves as example code to show how to use the Lucene spatial
 * module.
 */
public class SpatialExample extends LuceneTestCase {

  //Note: Test invoked via TestTestFramework.spatialExample()

  public static void main(String[] args) throws IOException {
    new SpatialExample().test();
  }

  public void test() throws IOException {
    init();
    indexPoints();
    search();
  }

  /**
   * The Spatial4j {@link SpatialContext} is a sort of global-ish singleton
   * needed by Lucene spatial.  It's a facade to the rest of Spatial4j, acting
   * as a factory for {@link Shape}s and provides access to reading and writing
   * them from Strings.
   */
  private SpatialContext ctx;//"ctx" is the conventional variable name

  /**
   * The Lucene spatial {@link SpatialStrategy} encapsulates an approach to
   * indexing and searching shapes, and providing distance values for them.
   * It's a simple API to unify different approaches. You might use more than
   * one strategy for a shape as each strategy has its strengths and weaknesses.
   * <p />
   * Note that these are initialized with a field name.
   */
  private SpatialStrategy strategy;

  private Directory directory;

  protected void init() {
    //Typical geospatial context
    //  These can also be constructed from SpatialContextFactory
    this.ctx = SpatialContext.GEO;

    int maxLevels = 11;//results in sub-meter precision for geohash
    //TODO demo lookup by detail distance
    //  This can also be constructed from SpatialPrefixTreeFactory
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, maxLevels);

    this.strategy = new RecursivePrefixTreeStrategy(grid, "myGeoField");

    this.directory = new RAMDirectory();
  }

  private void indexPoints() throws IOException {
    IndexWriterConfig iwConfig = new IndexWriterConfig(TEST_VERSION_CURRENT,null);
    IndexWriter indexWriter = new IndexWriter(directory, iwConfig);

    //Spatial4j is x-y order for arguments
    indexWriter.addDocument(newSampleDocument(
        2, ctx.makePoint(-80.93, 33.77)));

    //When parsing a string to a shape, the presence of a comma means it's y-x
    // order (lon, lat)
    indexWriter.addDocument(newSampleDocument(
        4, ctx.readShape("-50.7693246, 60.9289094")));

    indexWriter.addDocument(newSampleDocument(
        20, ctx.makePoint(0.1,0.1), ctx.makePoint(0, 0)));

    indexWriter.close();
  }

  private Document newSampleDocument(int id, Shape... shapes) {
    Document doc = new Document();
    doc.add(new IntField("id", id, Field.Store.YES));
    //Potentially more than one shape in this field is supported by some
    // strategies; see the javadocs of the SpatialStrategy impl to see.
    for (Shape shape : shapes) {
      for (Field f : strategy.createIndexableFields(shape)) {
        doc.add(f);
      }
      //store it too; the format is up to you
      doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));
    }

    return doc;
  }

  private void search() throws IOException {
    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
    Sort idSort = new Sort(new SortField("id", SortField.Type.INT));

    //--Filter by circle (<= distance from a point)
    {
      //Search with circle
      //note: SpatialArgs can be parsed from a string
      SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,
          ctx.makeCircle(-80.0, 33.0, DistanceUtils.dist2Degrees(200, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
      Filter filter = strategy.makeFilter(args);
      TopDocs docs = indexSearcher.search(new MatchAllDocsQuery(), filter, 10, idSort);
      assertDocMatchedIds(indexSearcher, docs, 2);
      //Now, lets get the distance for the 1st doc via computing from stored point value:
      // (this computation is usually not redundant)
      StoredDocument doc1 = indexSearcher.doc(docs.scoreDocs[0].doc);
      String doc1Str = doc1.getField(strategy.getFieldName()).stringValue();
      Point doc1Point = (Point) ctx.readShape(doc1Str);
      double doc1DistDEG = ctx.getDistCalc().distance(args.getShape().getCenter(), doc1Point);
      assertEquals(121.6d, DistanceUtils.degrees2Dist(doc1DistDEG, DistanceUtils.EARTH_MEAN_RADIUS_KM), 0.1);
    }
    //--Match all, order by distance ascending
    {
      Point pt = ctx.makePoint(60, -50);
      double degToKm = DistanceUtils.degrees2Dist(1, DistanceUtils.EARTH_MEAN_RADIUS_KM);
      ValueSource valueSource = strategy.makeDistanceValueSource(pt, degToKm);//the distance (in km)
      Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(indexSearcher);//false=asc dist
      TopDocs docs = indexSearcher.search(new MatchAllDocsQuery(), 10, distSort);
      assertDocMatchedIds(indexSearcher, docs, 4, 20, 2);
      //To get the distance, we could compute from stored values like earlier.
      // However in this example we sorted on it, and the distance will get
      // computed redundantly.  If the distance is only needed for the top-X
      // search results then that's not a big deal. Alternatively, try wrapping
      // the ValueSource with CachingDoubleValueSource then retrieve the value
      // from the ValueSource now. See LUCENE-4541 for an example.
    }
    //demo arg parsing
    {
      SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,
          ctx.makeCircle(-80.0, 33.0, 1));
      SpatialArgs args2 = new SpatialArgsParser().parse("Intersects(Circle(33,-80 d=1))", ctx);
      assertEquals(args.toString(),args2.toString());
    }

    indexReader.close();
  }

  private void assertDocMatchedIds(IndexSearcher indexSearcher, TopDocs docs, int... ids) throws IOException {
    int[] gotIds = new int[docs.totalHits];
    for (int i = 0; i < gotIds.length; i++) {
      gotIds[i] = indexSearcher.doc(docs.scoreDocs[i].doc).getField("id").numericValue().intValue();
    }
    assertArrayEquals(ids,gotIds);
  }

}
