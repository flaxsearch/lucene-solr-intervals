package org.apache.solr.handler.component;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;

import junit.framework.AssertionFailedError;

public class DistributedFacetPivotSmallTest extends BaseDistributedSearchTestCase {
  
  public DistributedFacetPivotSmallTest() {
    this.fixShardCount = true;
    this.shardCount = 4;
  }
  
  @Override
  public void doTest() throws Exception {
    
    del("*:*");

    // NOTE: we use the literal (4 character) string "null" as a company name
    // to help ensure there isn't any bugs where the literal string is treated as if it 
    // were a true NULL value.
    index(id, 19, "place_t", "cardiff dublin", "company_t", "microsoft polecat");
    index(id, 20, "place_t", "dublin", "company_t", "polecat microsoft null");
    index(id, 21, "place_t", "london la dublin", "company_t",
        "microsoft fujitsu null polecat");
    index(id, 22, "place_t", "krakow london cardiff", "company_t",
        "polecat null bbc");
    index(id, 23, "place_t", "london", "company_t", "");
    index(id, 24, "place_t", "la", "company_t", "");
    index(id, 25, "company_t", "microsoft polecat null fujitsu null bbc");
    index(id, 26, "place_t", "krakow", "company_t", "null");
    index(id, 27, "place_t", "krakow cardiff dublin london la", "company_t",
        "null microsoft polecat bbc fujitsu");
    index(id, 28, "place_t", "cork", "company_t",
        "fujitsu rte");
    commit();
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);    
    
    
    final ModifiableSolrParams params = new ModifiableSolrParams();
    setDistributedParams(params);
    params.add("q", "*:*");
    params.add("facet", "true");
    params.add("facet.pivot", "place_t,company_t");
   
    
    QueryResponse rsp = queryServer(params);
    
    List<PivotField> expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    List<PivotField> expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "microsoft", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "null", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    List<PivotField> expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat", 4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft", 4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "bbc", 1, null));
    List<PivotField> expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "microsoft", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    List<PivotField> expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "microsoft", 2,null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2,null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "null", 2, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "bbc", 1, null));
    expectedLAPivots.add(new ComparablePivotField("company_t", "polecat", 2,null));
    List<PivotField> expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "polecat",2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3,null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "microsoft", 1, null));
    List<PivotField> expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    expectedCorkPivots.add(new ComparablePivotField("company_t", "rte", 1, null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4, expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cardiff", 3,  expectedCardiffPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4, expectedLondonPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "la", 3, expectedLAPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "krakow", 3, expectedKrakowPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "cork", 1, expectedCorkPivots));
    
    
    List<PivotField> placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    // Useful to check for errors, orders lists and does toString() equality
    // check
    testOrderedPivotsStringEquality(expectedPlacePivots, placePivots);
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test sorting by count
    
    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_COUNT);
    
    rsp = queryServer(params);
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    testCountSorting(placePivots);
    
    // Test limit
    
    params.set(FacetParams.FACET_LIMIT, 2);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",
        4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",
        4, null));
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3,
        null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3,
        null));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4,
        expectedDublinPivots));
    expectedPlacePivots.add(new ComparablePivotField("place_t", "london", 4,
        expectedLondonPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");
    
    assertEquals(expectedPlacePivots, placePivots);
    
    // Test individual facet.limit values
    params.remove(FacetParams.FACET_LIMIT);
    
    params.set("f.place_t." + FacetParams.FACET_LIMIT, 1);
    params.set("f.company_t." + FacetParams.FACET_LIMIT, 4);
    
    rsp = queryServer(params);
    
    expectedPlacePivots = new UnorderedEqualityArrayList<PivotField>();
    
    expectedDublinPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedDublinPivots.add(new ComparablePivotField("company_t", "microsoft",4, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "polecat",4, null));    
    expectedDublinPivots.add(new ComparablePivotField("company_t", "null",3, null));
    expectedDublinPivots.add(new ComparablePivotField("company_t", "fujitsu",2, null));
    
    expectedLondonPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLondonPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "bbc", 2, null));
    expectedLondonPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    
    expectedCardiffPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCardiffPivots.add(new ComparablePivotField("company_t", "polecat", 3, null));
    
    expectedKrakowPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedKrakowPivots.add(new ComparablePivotField("company_t", "null", 3, null));
    
    expectedLAPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedLAPivots.add(new ComparablePivotField("company_t", "fujitsu", 2, null));
    
    expectedCorkPivots = new UnorderedEqualityArrayList<PivotField>();
    expectedCorkPivots.add(new ComparablePivotField("company_t", "fujitsu", 1, null));
    
    expectedPlacePivots.add(new ComparablePivotField("place_t", "dublin", 4, expectedDublinPivots));
    
    placePivots = rsp.getFacetPivot().get("place_t,company_t");    
    assertEquals(expectedPlacePivots, placePivots);
    
    params.remove("f.company_t." + FacetParams.FACET_LIMIT);
    params.remove("f.place_t." + FacetParams.FACET_LIMIT);
    params.set(FacetParams.FACET_LIMIT, 2);
    
    // Test facet.missing=true with diff sorts

    index("id",777); // NOTE: id=25 has no place as well
    commit();

    SolrParams missingA = params( "q", "*:*",
                                  "rows", "0",
                                  "facet","true",
                                  "facet.pivot","place_t,company_t",
                                  // default facet.sort
                                  FacetParams.FACET_MISSING, "true" );
    SolrParams missingB = SolrParams.wrapDefaults(missingA, 
                                                  params(FacetParams.FACET_LIMIT, "4",
                                                         "facet.sort", "index"));
    for (SolrParams p : new SolrParams[] { missingA, missingB }) {
      // in either case, the last pivot option should be the same
      rsp = query( p );
      placePivots = rsp.getFacetPivot().get("place_t,company_t");
      assertTrue("not enough values for pivot: " + p + " => " + placePivots, 
                 1 < placePivots.size());
      PivotField missing = placePivots.get(placePivots.size()-1);
      assertNull("not the missing place value: " + p, missing.getValue());
      assertEquals("wrong missing place count: " + p, 2, missing.getCount());
      assertTrue("not enough sub-pivots for missing place: "+ p +" => " + missing.getPivot(),
                 1 < missing.getPivot().size());
      missing = missing.getPivot().get(missing.getPivot().size()-1);
      assertNull("not the missing company value: " + p, missing.getValue());
      assertEquals("wrong missing company count: " + p, 1, missing.getCount());
      assertNull("company shouldn't have sub-pivots: " + p, missing.getPivot());
    }

    // sort=index + mincount + limit
    for (SolrParams variableParams : new SolrParams[] { 
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count","0",
               "facet.overrequest.ratio","0"),
        params()                                  }) {


      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.pivot","company_t",
                                                      "facet.sort", "index",
                                                      "facet.pivot.mincount", "4",
                                                      "facet.limit", "4"),
                                              variableParams );

      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(4, pivots.size());
        assertEquals("fujitsu", pivots.get(0).getValue());
        assertEquals(4, pivots.get(0).getCount());
        assertEquals("microsoft", pivots.get(1).getValue());
        assertEquals(5, pivots.get(1).getCount());
        assertEquals("null", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());
        assertEquals("polecat", pivots.get(3).getValue());
        assertEquals(6, pivots.get(3).getCount());
        
      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }

    // sort=index + mincount + limit + offset
    for (SolrParams variableParams : new SolrParams[] { 
        // we should get the same results regardless of overrequest
        params("facet.overrequest.count","0",
               "facet.overrequest.ratio","0"),
        params()                                  }) {

      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.pivot","company_t",
                                                      "facet.sort", "index",
                                                      "facet.pivot.mincount", "4",
                                                      "facet.offset", "1",
                                                      "facet.limit", "4"),
                                              variableParams );
      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(3, pivots.size()); // asked for 4, but not enough meet the mincount
        assertEquals("microsoft", pivots.get(0).getValue());
        assertEquals(5, pivots.get(0).getCount());
        assertEquals("null", pivots.get(1).getValue());
        assertEquals(6, pivots.get(1).getCount());
        assertEquals("polecat", pivots.get(2).getValue());
        assertEquals(6, pivots.get(2).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }

    }
      
    // sort=index + mincount + limit + offset (more permutations)
    for (SolrParams variableParams : new SolrParams[] { 
        // all of these combinations should result in the same first value
        params("facet.pivot.mincount", "4",
               "facet.offset", "2"),
        params("facet.pivot.mincount", "5",
               "facet.offset", "1"),
        params("facet.pivot.mincount", "6",
               "facet.offset", "0" )                  }) {
      
      SolrParams p = SolrParams.wrapDefaults( params( "q", "*:*",
                                                      "rows", "0",
                                                      "facet","true",
                                                      "facet.limit","1",
                                                      "facet.sort","index",
                                                      "facet.overrequest.ratio","0",
                                                      "facet.pivot", "company_t"),
                                              variableParams );

      try {
        List<PivotField> pivots = query( p ).getFacetPivot().get("company_t");
        assertEquals(1, pivots.size());
        assertEquals(pivots.toString(), "null", pivots.get(0).getValue());
        assertEquals(pivots.toString(), 6, pivots.get(0).getCount());

      } catch (AssertionFailedError ae) {
        throw new AssertionError(ae.getMessage() + " <== " + p.toString(), ae);
      }
    }
  }

  // Useful to check for errors, orders lists and does toString() equality check
  private void testOrderedPivotsStringEquality(
      List<PivotField> expectedPlacePivots, List<PivotField> placePivots) {
    Collections.sort(expectedPlacePivots, new PivotFieldComparator());
    for (PivotField expectedPivot : expectedPlacePivots) {
      if (expectedPivot.getPivot() != null) {
        Collections.sort(expectedPivot.getPivot(), new PivotFieldComparator());
      }
    }
    Collections.sort(placePivots, new PivotFieldComparator());
    for (PivotField pivot : placePivots) {
      if (pivot.getPivot() != null) {
        Collections.sort(pivot.getPivot(), new PivotFieldComparator());
      }
    }
    assertEquals(expectedPlacePivots.toString(), placePivots.toString());
  }
  
  private void testCountSorting(List<PivotField> pivots) {
    Integer lastCount = null;
    for (PivotField pivot : pivots) {
      if (lastCount != null) {
        assertTrue(pivot.getCount() <= lastCount);
      }
      lastCount = pivot.getCount();
      if (pivot.getPivot() != null) {
        testCountSorting(pivot.getPivot());
      }
    }
  }
  
  public static class ComparablePivotField extends PivotField {
    

    public ComparablePivotField(String f, Object v, int count,
        List<PivotField> pivot) {
      super(f,v,count,pivot);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (!obj.getClass().isAssignableFrom(PivotField.class)) return false;
      PivotField other = (PivotField) obj;
      if (getCount() != other.getCount()) return false;
      if (getField() == null) {
        if (other.getField() != null) return false;
      } else if (!getField().equals(other.getField())) return false;
      if (getPivot() == null) {
        if (other.getPivot() != null) return false;
      } else if (!getPivot().equals(other.getPivot())) return false;
      if (getValue() == null) {
        if (other.getValue() != null) return false;
      } else if (!getValue().equals(other.getValue())) return false;
      return true;
    }
  }
  
  public static class UnorderedEqualityArrayList<T> extends ArrayList<T> {
    
    @Override
    public boolean equals(Object o) {
      boolean equal = false;
      if (o instanceof ArrayList) {
        List<?> otherList = (List<?>) o;
        if (size() == otherList.size()) {
          equal = true;
          for (Object objectInOtherList : otherList) {
            if (!contains(objectInOtherList)) {
              equal = false;
            }
          }
        }
      }
      return equal;
    }
    
    public int indexOf(Object o) {
      for (int i = 0; i < size(); i++) {
        if (get(i).equals(o)) {
          return i;
        }
      }
      return -1;
    }
  }
  
  public class PivotFieldComparator implements Comparator<PivotField> {
    
    @Override
    public int compare(PivotField o1, PivotField o2) {
      Integer compare = (Integer.valueOf(o2.getCount())).compareTo(Integer
          .valueOf(o1.getCount()));
      if (compare == 0) {
        compare = ((String) o2.getValue()).compareTo((String) o1.getValue());
      }
      return compare;
    }
    
  }
  
}
