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

package org.apache.solr.search;


import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class TestFiltering extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTests() throws Exception {
    System.setProperty("enable.update.log", "false"); // schema12 doesn't support _version_
    initCore("solrconfig.xml","schema12.xml");
  }


  public void testCaching() throws Exception {
    clearIndex();
    assertU(adoc("id","4", "val_i","1"));
    assertU(adoc("id","1", "val_i","2"));
    assertU(adoc("id","3", "val_i","3"));
    assertU(adoc("id","2", "val_i","4"));
    assertU(commit());

    int prevCount;

    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("q","*:*", "fq","{!frange l=2 u=3 cache=false cost=100}val_i")
        ,"/response/numFound==2"
    );
    assertEquals(1, DelegatingCollector.setLastDelegateCount - prevCount);

    // The exact same query the second time will be cached by the queryCache
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("q","*:*", "fq","{!frange l=2 u=3 cache=false cost=100}val_i")
        ,"/response/numFound==2"
    );
    assertEquals(0, DelegatingCollector.setLastDelegateCount - prevCount);

    // cache is true by default
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("q","*:*", "fq","{!frange l=2 u=4}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(0, DelegatingCollector.setLastDelegateCount - prevCount);

    // default cost avoids post filtering
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("q","*:*", "fq","{!frange l=2 u=5 cache=false}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(0, DelegatingCollector.setLastDelegateCount - prevCount);


    // now re-do the same tests w/ faceting on to get the full docset

    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("facet","true", "facet.field","id", "q","*:*", "fq","{!frange l=2 u=6 cache=false cost=100}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(1, DelegatingCollector.setLastDelegateCount - prevCount);

    // since we need the docset and the filter was not cached, the collector will need to be used again
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("facet","true", "facet.field","id", "q","*:*", "fq","{!frange l=2 u=6 cache=false cost=100}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(1, DelegatingCollector.setLastDelegateCount - prevCount);

    // cache is true by default
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("facet","true", "facet.field","id", "q","*:*", "fq","{!frange l=2 u=7}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(0, DelegatingCollector.setLastDelegateCount - prevCount);

    // default cost avoids post filtering
    prevCount = DelegatingCollector.setLastDelegateCount;
    assertJQ(req("facet","true", "facet.field","id", "q","*:*", "fq","{!frange l=2 u=8 cache=false}val_i")
        ,"/response/numFound==3"
    );
    assertEquals(0, DelegatingCollector.setLastDelegateCount - prevCount);

    // test that offset works when not caching main query
    assertJQ(req("q","{!cache=false}*:*", "start","2", "rows","1", "sort","val_i asc", "fl","val_i")
        ,"/response/docs==[{'val_i':3}]"
    );

  }


  class Model {
    int indexSize;
    FixedBitSet answer;
    FixedBitSet multiSelect;
    FixedBitSet facetQuery;

    void clear() {
      answer = new FixedBitSet(indexSize);
      answer.set(0, indexSize);

      multiSelect = new FixedBitSet(indexSize);
      multiSelect.set(0, indexSize);

      facetQuery = new FixedBitSet(indexSize);
      facetQuery.set(0, indexSize);
    }
  }

  static String f = "val_i";

  String frangeStr(boolean negative, int l, int u, boolean cache, int cost, boolean exclude) {

    String topLev="";
    if (!cache || exclude) {
      topLev = "" + (cache || random().nextBoolean() ? " cache="+cache : "")
        + (cost!=0 ? " cost="+cost : "")
        + ((exclude) ? " tag=t" : "");
    }

    String ret = "{!frange v="+f+" l="+l+" u="+u;
    if (negative) {
      ret = "-_query_:\"" + ret + "}\"";
      if (topLev.length()>0) {
        ret = "{!" + topLev + "}" + ret; // add options at top level (can't be on frange)
      }
    } else {
      ret += topLev + "}"; // add options right to frange
    }

    return ret;
  }

  String makeRandomQuery(Model model, boolean mainQuery, boolean facetQuery) {

    boolean cache = random().nextBoolean();
    int cost = cache ? 0 : random().nextBoolean() ? random().nextInt(200) : 0;
    boolean positive = random().nextBoolean();
    boolean exclude = facetQuery ? false : random().nextBoolean();    // can't exclude a facet query from faceting

    FixedBitSet[] sets = facetQuery ? new FixedBitSet[]{model.facetQuery} :
        (exclude ? new FixedBitSet[]{model.answer, model.facetQuery} : new FixedBitSet[]{model.answer, model.multiSelect, model.facetQuery});

    if (random().nextInt(100) < 50) {
      // frange
      int l=0;
      int u=0;

      if (positive) {
        // positive frange, make it big by taking the max of 4 tries
        int n=-1;

        for (int i=0; i<4; i++) {
          int ll = random().nextInt(model.indexSize);
          int uu = ll + ((ll==model.indexSize-1) ? 0 : random().nextInt(model.indexSize-l));
          if (uu-ll+1 > n) {
            n = uu-ll+1;
            u = uu;
            l = ll;
          }
        }

        for (FixedBitSet set : sets) {
          set.clear(0,l);
          if (u + 1 < model.indexSize) {
            set.clear(u+1, model.indexSize);
          }
        }
      } else {
        // negative frange.. make it relatively small
        l = random().nextInt(model.indexSize);
        u = Math.max(model.indexSize-1, l+random().nextInt(Math.max(model.indexSize / 10, 2)));

        for (FixedBitSet set : sets) {
          int end = Math.min(u+1, set.length());
          set.clear(l,end);
        }
      }

      return frangeStr(!positive, l, u, cache, cost, exclude);
    } else {
      // term or boolean query
      FixedBitSet pset = new FixedBitSet(model.indexSize);
      for (int i=0; i<pset.getBits().length; i++) {
        pset.getBits()[i] = random().nextLong();    // set 50% of the bits on average
      }
      if (positive) {
        for (FixedBitSet set : sets) {
          set.and(pset);
        }
      } else {
        for (FixedBitSet set : sets) {
          set.andNot(pset);
        }
      }


      StringBuilder sb = new StringBuilder();
      for (int doc=-1;;) {
        if (doc+1 >= model.indexSize) break;
        doc = pset.nextSetBit(doc+1);
        if (doc < 0) break;
        sb.append((positive ? " ":" -") + f+":"+doc);
      }

      String ret = sb.toString();
      if (ret.length()==0) ret = (positive ? "":"-") + "id:99999999";

      if (!cache || exclude || random().nextBoolean()) {
        ret = "{!cache=" + cache
            + ((cost != 0) ? " cost="+cost : "")
            + ((exclude) ? " tag=t" : "")
            + "}" + ret;
      }

      return ret;
    }
  }

  @Test
  public void testRandomFiltering() throws Exception {
    int indexIter=5 * RANDOM_MULTIPLIER;
    int queryIter=250 * RANDOM_MULTIPLIER;
    Model model = new Model();

    for (int iiter = 0; iiter<indexIter; iiter++) {
      model.indexSize = random().nextInt(20 * RANDOM_MULTIPLIER) + 1;
      clearIndex();

      for (int i=0; i<model.indexSize; i++) {
        String val = Integer.toString(i);

        assertU(adoc("id",val,f,val));
        if (random().nextInt(100) < 20) {
          // duplicate doc 20% of the time (makes deletions)
          assertU(adoc("id",val,f,val));
        }
        if (random().nextInt(100) < 10) {
          // commit 10% of the time (forces a new segment)
          assertU(commit());
        }
      }
      assertU(commit());
      // sanity check
      assertJQ(req("q", "*:*"), "/response/numFound==" + model.indexSize);

      int totalMatches=0;
      int nonZeros=0;
      for (int qiter=0; qiter<queryIter; qiter++) {
        model.clear();
        List<String> params = new ArrayList<>();
        params.add("q"); params.add(makeRandomQuery(model, true, false));

        int nFilters = random().nextInt(5);
        for (int i=0; i<nFilters; i++) {
          params.add("fq");  params.add(makeRandomQuery(model, false, false));
        }

        boolean facet = random().nextBoolean();
        if (facet) {
          // basic facet.query tests getDocListAndSet
          params.add("facet"); params.add("true");
          params.add("facet.query"); params.add("*:*");
          params.add("facet.query"); params.add("{!key=multiSelect ex=t}*:*");

          String facetQuery = makeRandomQuery(model, false, true);
          if (facetQuery.startsWith("{!")) {
            facetQuery = "{!key=facetQuery " + facetQuery.substring(2);
          } else {
            facetQuery = "{!key=facetQuery}" + facetQuery;
          }
          params.add("facet.query"); params.add(facetQuery);
        }

        if (random().nextInt(100) < 10) {
          params.add("group"); params.add("true");
          params.add("group.main"); params.add("true");
          params.add("group.field"); params.add("id");
          if (random().nextBoolean()) {
            params.add("group.cache.percent"); params.add("100");
          }
        }

        SolrQueryRequest sreq = req(params.toArray(new String[params.size()]));
        long expected = model.answer.cardinality();
        long expectedMultiSelect = model.multiSelect.cardinality();
        long expectedFacetQuery = model.facetQuery.cardinality();

        totalMatches += expected;
        if (expected > 0) {
          nonZeros++;
        }

        if (iiter==-1 && qiter==-1) {
          // set breakpoint here to debug a specific issue
          System.out.println("request="+params);
        }

        try {
          assertJQ(sreq
              ,"/response/numFound==" + expected
              , facet ? "/facet_counts/facet_queries/*:*/==" + expected : null
              , facet ? "/facet_counts/facet_queries/multiSelect/==" + expectedMultiSelect : null
              , facet ? "/facet_counts/facet_queries/facetQuery/==" + expectedFacetQuery : null
          );
        } catch (Exception e) {
          // show the indexIter and queryIter for easier debugging
          SolrException.log(log, e);
          String s= "FAILURE: indexSize=" + model.indexSize + " iiter=" + iiter + " qiter=" + qiter + " request="+params;
          log.error(s);
          fail(s);
        }
      }

      // After making substantial changes to this test, make sure that we still get a
      // decent number of queries that match some documents
      // System.out.println("totalMatches=" + totalMatches + " nonZeroQueries="+nonZeros);

    }
  }

  public void testHossssSanity() throws Exception {
    clearIndex();

    SolrParams match_0 
      = params("q",  "{!frange v=val_i l=0 u=1}",
               "fq", "{!frange v=val_i l=1 u=1}",
               "fq", "{!frange v=val_i l=0 u=1}",
               "fq", "-_query_:\"{!frange v=val_i l=1 u=1}\"",
               "fq", "-_query_:\"{!frange v=val_i l=0 u=1}\"");
    
    SolrParams match_1
      = params("q",  "{!frange v=val_i l=0 u=1}",
               "fq", "{!frange v=val_i l=0 u=1}",
               "fq", "{!frange v=val_i l=0 u=1}",
               "fq", "-_query_:\"{!frange v=val_i l=1 u=1}\"",
               "fq", "-_query_:\"{!frange v=val_i l=1 u=1}\"");
    
    final int numDocs = 10;

    for (int i = 0; i < numDocs; i++) {
      String val = Integer.toString(i);
      assertU(adoc("id",val,f,val));
    }
    assertU(commit());

    // sanity check
    assertJQ(req("q", "*:*"), "/response/numFound==" + numDocs);

    // 1 then 0
    assertJQ(req(match_1), "/response/numFound==1");
    assertJQ(req(match_0), "/response/numFound==0");

    // clear caches
    assertU(commit());

    // 0 then 1
    assertJQ(req(match_0), "/response/numFound==0");
    assertJQ(req(match_1), "/response/numFound==1");
  }
}
