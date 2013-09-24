package org.apache.lucene.facet.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.old.AdaptiveFacetsAccumulator;
import org.apache.lucene.facet.old.OldFacetsAccumulator;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.PerDimensionIndexingParams;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.sampling.RandomSampler;
import org.apache.lucene.facet.sampling.Sampler;
import org.apache.lucene.facet.sampling.SamplingAccumulator;
import org.apache.lucene.facet.sampling.SamplingParams;
import org.apache.lucene.facet.sampling.SamplingWrapper;
import org.apache.lucene.facet.sampling.TakmiSampleFixer;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.junit.Test;

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

public class TestFacetsCollector extends FacetTestCase {

  @Test
  public void testSumScoreAggregator() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();

    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

    FacetFields facetFields = new FacetFields(taxonomyWriter);
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      if (random().nextBoolean()) { // don't match all documents
        doc.add(new StringField("f", "v", Store.NO));
      }
      facetFields.addFields(doc, Collections.singletonList(new CategoryPath("a")));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams sParams = new FacetSearchParams(new SumScoreFacetRequest(new CategoryPath("a"), 10));
    TaxonomyFacetsAccumulator fa = new TaxonomyFacetsAccumulator(sParams, r, taxo) {
      @Override
      public FacetsAggregator getAggregator() {
        return new SumScoreFacetsAggregator();
      }
    };
    FacetsCollector fc = FacetsCollector.create(fa);
    TopScoreDocCollector topDocs = TopScoreDocCollector.create(10, false);
    ConstantScoreQuery csq = new ConstantScoreQuery(new MatchAllDocsQuery());
    csq.setBoost(2.0f);
    
    newSearcher(r).search(csq, MultiCollector.wrap(fc, topDocs));
    
    List<FacetResult> res = fc.getFacetResults();
    float value = (float) res.get(0).getFacetResultNode().value;
    TopDocs td = topDocs.topDocs();
    int expected = (int) (td.getMaxScore() * td.totalHits);
    assertEquals(expected, (int) value);
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testMultiCountingLists() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetIndexingParams fip = new PerDimensionIndexingParams(Collections.singletonMap(new CategoryPath("b"), new CategoryListParams("$b")));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter, fip);
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new StringField("f", "v", Store.NO));
      List<CategoryPath> cats = new ArrayList<CategoryPath>();
      cats.add(new CategoryPath("a"));
      cats.add(new CategoryPath("b"));
      facetFields.addFields(doc, cats);
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams sParams = new FacetSearchParams(fip,
        new CountFacetRequest(new CategoryPath("a"), 10), 
        new CountFacetRequest(new CategoryPath("b"), 10));
    FacetsCollector fc = FacetsCollector.create(sParams, r, taxo);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    for (FacetResult res : fc.getFacetResults()) {
      assertEquals("unexpected count for " + res, r.maxDoc(), (int) res.getFacetResultNode().value);
    }
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testCountAndSumScore() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    FacetIndexingParams fip = new PerDimensionIndexingParams(Collections.singletonMap(new CategoryPath("b"), new CategoryListParams("$b")));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter, fip);
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      doc.add(new StringField("f", "v", Store.NO));
      List<CategoryPath> cats = new ArrayList<CategoryPath>();
      cats.add(new CategoryPath("a"));
      cats.add(new CategoryPath("b"));
      facetFields.addFields(doc, cats);
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams sParams = new FacetSearchParams(fip,
        new CountFacetRequest(new CategoryPath("a"), 10), 
        new SumScoreFacetRequest(new CategoryPath("b"), 10));
    
    FacetsCollector fc = FacetsCollector.create(sParams, r, taxo);
    TopScoreDocCollector topDocs = TopScoreDocCollector.create(10, false);
    newSearcher(r).search(new MatchAllDocsQuery(), MultiCollector.wrap(fc, topDocs));
    
    List<FacetResult> facetResults = fc.getFacetResults();
    FacetResult fresA = facetResults.get(0);
    assertEquals("unexpected count for " + fresA, r.maxDoc(), (int) fresA.getFacetResultNode().value);
    
    FacetResult fresB = facetResults.get(1);
    double expected = topDocs.topDocs().getMaxScore() * r.numDocs();
    assertEquals("unexpected value for " + fresB, expected, fresB.getFacetResultNode().value, 1E-10);
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testCountRoot() throws Exception {
    // LUCENE-4882: FacetsAccumulator threw NPE if a FacetRequest was defined on CP.EMPTY
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter);
    for(int i = atLeast(30); i > 0; --i) {
      Document doc = new Document();
      facetFields.addFields(doc, Arrays.asList(new CategoryPath("a"), new CategoryPath("b")));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(CategoryPath.EMPTY, 10));
    
    final TaxonomyFacetsAccumulator fa = random().nextBoolean() ? new TaxonomyFacetsAccumulator(fsp, r, taxo) : new OldFacetsAccumulator(fsp, r, taxo);
    FacetsCollector fc = FacetsCollector.create(fa);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    FacetResult res = fc.getFacetResults().get(0);
    for (FacetResultNode node : res.getFacetResultNode().subResults) {
      assertEquals(r.numDocs(), (int) node.value);
    }
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }

  @Test
  public void testGetFacetResultsTwice() throws Exception {
    // LUCENE-4893: counts were multiplied as many times as getFacetResults was called.
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter);
    Document doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("a/1", '/'), new CategoryPath("b/1", '/')));
    iw.addDocument(doc);
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("a"), 10), 
        new CountFacetRequest(new CategoryPath("b"), 10));
    final TaxonomyFacetsAccumulator fa = random().nextBoolean() ? new TaxonomyFacetsAccumulator(fsp, r, taxo) : new OldFacetsAccumulator(fsp, r, taxo);
    final FacetsCollector fc = FacetsCollector.create(fa);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> res1 = fc.getFacetResults();
    List<FacetResult> res2 = fc.getFacetResults();
    assertSame("calling getFacetResults twice should return the exact same result", res1, res2);
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testReset() throws Exception {
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter);
    Document doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("a/1", '/'), new CategoryPath("b/1", '/')));
    iw.addDocument(doc);
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("a"), 10), 
        new CountFacetRequest(new CategoryPath("b"), 10));
    final TaxonomyFacetsAccumulator fa = random().nextBoolean() ? new TaxonomyFacetsAccumulator(fsp, r, taxo) : new OldFacetsAccumulator(fsp, r, taxo);
    final FacetsCollector fc = FacetsCollector.create(fa);
    // this should populate the cached results, but doing search should clear the cache
    fc.getFacetResults();
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> res1 = fc.getFacetResults();
    // verify that we didn't get the cached result
    assertEquals(2, res1.size());
    for (FacetResult res : res1) {
      assertEquals(1, res.getFacetResultNode().subResults.size());
      assertEquals(1, (int) res.getFacetResultNode().subResults.get(0).value);
    }
    fc.reset();
    List<FacetResult> res2 = fc.getFacetResults();
    assertNotSame("reset() should clear the cached results", res1, res2);
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testParentOrdinal() throws Exception {
    // LUCENE-4913: root ordinal was always 0 when all children were requested
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter);
    Document doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("a/1", '/')));
    iw.addDocument(doc);
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);

    // assert IntFacetResultHandler
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("a"), 10));
    TaxonomyFacetsAccumulator fa = random().nextBoolean() ? new TaxonomyFacetsAccumulator(fsp, r, taxo) : new OldFacetsAccumulator(fsp, r, taxo);
    FacetsCollector fc = FacetsCollector.create(fa);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    assertTrue("invalid ordinal for child node: 0", 0 != fc.getFacetResults().get(0).getFacetResultNode().subResults.get(0).ordinal);
    
    // assert IntFacetResultHandler
    fsp = new FacetSearchParams(new SumScoreFacetRequest(new CategoryPath("a"), 10));
    if (random().nextBoolean()) {
      fa = new TaxonomyFacetsAccumulator(fsp, r, taxo) {
        @Override
        public FacetsAggregator getAggregator() {
          return new SumScoreFacetsAggregator();
        }
      };
    } else {
      fa = new OldFacetsAccumulator(fsp, r, taxo);
    }
    fc = FacetsCollector.create(fa);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    assertTrue("invalid ordinal for child node: 0", 0 != fc.getFacetResults().get(0).getFacetResultNode().subResults.get(0).ordinal);
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }
  
  @Test
  public void testNumValidDescendants() throws Exception {
    // LUCENE-4885: FacetResult.numValidDescendants was not set properly by FacetsAccumulator
    Directory indexDir = newDirectory();
    Directory taxoDir = newDirectory();
    
    TaxonomyWriter taxonomyWriter = new DirectoryTaxonomyWriter(taxoDir);
    IndexWriter iw = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    
    FacetFields facetFields = new FacetFields(taxonomyWriter);
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      facetFields.addFields(doc, Arrays.asList(new CategoryPath("a", Integer.toString(i))));
      iw.addDocument(doc);
    }
    
    taxonomyWriter.close();
    iw.close();
    
    DirectoryReader r = DirectoryReader.open(indexDir);
    DirectoryTaxonomyReader taxo = new DirectoryTaxonomyReader(taxoDir);
    
    CountFacetRequest cfr = new CountFacetRequest(new CategoryPath("a"), 2);
    cfr.setResultMode(random().nextBoolean() ? ResultMode.GLOBAL_FLAT : ResultMode.PER_NODE_IN_TREE);
    FacetSearchParams fsp = new FacetSearchParams(cfr);
    final TaxonomyFacetsAccumulator fa = random().nextBoolean() ? new TaxonomyFacetsAccumulator(fsp, r, taxo) : new OldFacetsAccumulator(fsp, r, taxo);
    FacetsCollector fc = FacetsCollector.create(fa);
    newSearcher(r).search(new MatchAllDocsQuery(), fc);
    
    FacetResult res = fc.getFacetResults().get(0);
    assertEquals(10, res.getNumValidDescendants());
    
    IOUtils.close(taxo, taxoDir, r, indexDir);
  }

  @Test
  public void testLabeling() throws Exception {
    Directory indexDir = newDirectory(), taxoDir = newDirectory();

    // create the index
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
    FacetFields facetFields = new FacetFields(taxoWriter);
    Document doc = new Document();
    facetFields.addFields(doc, Arrays.asList(new CategoryPath("A/1", '/')));
    indexWriter.addDocument(doc);
    IOUtils.close(indexWriter, taxoWriter);
    
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    // ask to count a non-existing category to test labeling
    FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("B"), 5));
    
    final SamplingParams sampleParams = new SamplingParams();
    sampleParams.setMaxSampleSize(100);
    sampleParams.setMinSampleSize(100);
    sampleParams.setSamplingThreshold(100);
    sampleParams.setOversampleFactor(1.0d);
    if (random().nextBoolean()) {
      sampleParams.setSampleFixer(new TakmiSampleFixer(indexReader, taxoReader, fsp));
    }
    final Sampler sampler = new RandomSampler(sampleParams, random());
    
    TaxonomyFacetsAccumulator[] accumulators = new TaxonomyFacetsAccumulator[] {
      new TaxonomyFacetsAccumulator(fsp, indexReader, taxoReader),
      new OldFacetsAccumulator(fsp, indexReader, taxoReader),
      new SamplingAccumulator(sampler, fsp, indexReader, taxoReader),
      new AdaptiveFacetsAccumulator(fsp, indexReader, taxoReader),
      new SamplingWrapper(new OldFacetsAccumulator(fsp, indexReader, taxoReader), sampler)
    };
    
    for (TaxonomyFacetsAccumulator fa : accumulators) {
      FacetsCollector fc = FacetsCollector.create(fa);
      searcher.search(new MatchAllDocsQuery(), fc);
      List<FacetResult> facetResults = fc.getFacetResults();
      assertNotNull(facetResults);
      assertEquals("incorrect label returned for " + fa, fsp.facetRequests.get(0).categoryPath, facetResults.get(0).getFacetResultNode().label);
    }
    
    try {
      // SortedSetDocValuesAccumulator cannot even be created in such state
      assertNull(new SortedSetDocValuesAccumulator(new SortedSetDocValuesReaderState(indexReader), fsp));
      // if this ever changes, make sure FacetResultNode is labeled correctly 
      fail("should not have succeeded to execute a request over a category which wasn't indexed as SortedSetDVField");
    } catch (IllegalArgumentException e) {
      // expected
    }

    RangeAccumulator ra = new RangeAccumulator(new RangeFacetRequest<LongRange>("f", new LongRange("grr", 0, true, 1, true)));
    FacetsCollector fc = FacetsCollector.create(ra);
    searcher.search(new MatchAllDocsQuery(), fc);
    List<FacetResult> facetResults = fc.getFacetResults();
    assertNotNull(facetResults);
    assertEquals("incorrect label returned for RangeAccumulator", new CategoryPath("f"), facetResults.get(0).getFacetResultNode().label);

    IOUtils.close(indexReader, taxoReader);

    IOUtils.close(indexDir, taxoDir);
  }

}
