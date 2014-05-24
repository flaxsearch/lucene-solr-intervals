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

package org.apache.solr.request;

import org.noggit.ObjectBuilder;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.util.TimeZoneUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;


public class SimpleFacetsTest extends SolrTestCaseJ4 {


  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    createIndex();
  }

  static int random_commit_percent = 30;
  static int random_dupe_percent = 25;   // some duplicates in the index to create deleted docs

  static void randomCommit(int percent_chance) {
    if (random().nextInt(100) <= percent_chance)
      assertU(commit());
  }

  static ArrayList<String[]> pendingDocs = new ArrayList<>();

  // committing randomly gives different looking segments each time
  static void add_doc(String... fieldsAndValues) {
    do {
      pendingDocs.add(fieldsAndValues);      
    } while (random().nextInt(100) <= random_dupe_percent);

    // assertU(adoc(fieldsAndValues));
    // randomCommit(random_commit_percent);
  }


  static void createIndex() throws Exception {
    doEmptyFacetCounts();   // try on empty index

    indexSimpleFacetCounts();
    indexDateFacets();
    indexFacetSingleValued();
    indexFacetPrefixMultiValued();
    indexFacetPrefixSingleValued();
    indexSimpleGroupedFacetCounts();

    Collections.shuffle(pendingDocs, random());
    for (String[] doc : pendingDocs) {
      assertU(adoc(doc));
      randomCommit(random_commit_percent);
    }
    assertU(commit());
  }

  static void indexSimpleFacetCounts() {
    add_doc("id", "42", 
            "range_facet_f", "35.3", 
            "trait_s", "Tool", "trait_s", "Obnoxious",
            "name", "Zapp Brannigan",
             "foo_s","A", "foo_s","B"
    );
    add_doc("id", "43" ,
            "range_facet_f", "28.789", 
            "title", "Democratic Order of Planets",
            "foo_s","A", "foo_s","B"
    );
    add_doc("id", "44", 
            "range_facet_f", "15.97", 
            "trait_s", "Tool",
            "name", "The Zapper",
            "foo_s","A", "foo_s","B", "foo_s","C"
    );
    add_doc("id", "45", 
            "range_facet_f", "30.0", 
            "trait_s", "Chauvinist",
            "title", "25 star General",
            "foo_s","A", "foo_s","B"
    );
    add_doc("id", "46", 
            "range_facet_f", "20.0", 
            "trait_s", "Obnoxious",
            "subject", "Defeated the pacifists of the Gandhi nebula",
            "foo_s","A", "foo_s","B"
    );
    add_doc("id", "47", 
            "range_facet_f", "28.62", 
            "trait_s", "Pig",
            "text", "line up and fly directly at the enemy death cannons, clogging them with wreckage!",
            "zerolen_s","",
            "foo_s","A", "foo_s","B", "foo_s","C"
    );
  }

  static void indexSimpleGroupedFacetCounts() {
    add_doc("id", "2000", "hotel_s1", "a", "airport_s1", "ams", "duration_i1", "5");
    add_doc("id", "2001", "hotel_s1", "a", "airport_s1", "dus", "duration_i1", "10");
    add_doc("id", "2002", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "10");
    add_doc("id", "2003", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "5");
    add_doc("id", "2004", "hotel_s1", "b", "airport_s1", "ams", "duration_i1", "5");
  }

  @Test
  public void testCachingBigTerms() throws Exception {
    assertQ( req("indent","true", "q", "id:[42 TO 47]",
            "facet", "true",
            "facet.field", "foo_s"  // big terms should cause foo_s:A to be cached
             ),
        "*[count(//doc)=6]"
    );

    // now use the cached term as a filter to make sure deleted docs are accounted for
    assertQ( req("indent","true", "fl","id", "q", "foo_s:B",
        "facet", "true",
        "facet.field", "foo_s",
        "fq","foo_s:A"
    ),
        "*[count(//doc)=6]"
    );


  }


  @Test
  public void testSimpleGroupedQueryRangeFacets() throws Exception {
    // for the purposes of our test data, it shouldn't matter 
    // if we use facet.limit -100, -1, or 100 ...
    // our set of values is small enough either way
    testSimpleGroupedQueryRangeFacets("-100");
    testSimpleGroupedQueryRangeFacets("-1");
    testSimpleGroupedQueryRangeFacets("100");
  }

  private void testSimpleGroupedQueryRangeFacets(String facetLimit) {
    assertQ(
        req(
            "q", "*:*",
            "fq", "id:[2000 TO 2004]",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.query", "airport_s1:ams"
        ),
        "//lst[@name='facet_queries']/int[@name='airport_s1:ams'][.='2']"
    );
    /* Testing facet.query using tagged filter query and exclusion */
    assertQ(
        req(
            "q", "*:*",
            "fq", "id:[2000 TO 2004]",
            "fq", "{!tag=dus}airport_s1:dus",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.query", "{!ex=dus}airport_s1:ams"
        ),
        "//lst[@name='facet_queries']/int[@name='{!ex=dus}airport_s1:ams'][.='2']"
    );
    assertQ(
        req(
            "q", "*:*",
            "fq", "id:[2000 TO 2004]",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.range", "duration_i1",
            "facet.range.start", "5",
            "facet.range.end", "11",
            "facet.range.gap", "1"
        ),
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='5'][.='2']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='6'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='7'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='8'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='9'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='10'][.='2']"
    );
    /* Testing facet.range using tagged filter query and exclusion */
    assertQ(
        req(
            "q", "*:*",
            "fq", "id:[2000 TO 2004]",
            "fq", "{!tag=dus}airport_s1:dus",
            "group", "true",
            "group.facet", "true",
            "group.field", "hotel_s1",
            "facet", "true",
            "facet.limit", facetLimit,
            "facet.range", "{!ex=dus}duration_i1",
            "facet.range.start", "5",
            "facet.range.end", "11",
            "facet.range.gap", "1"
        ),
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='5'][.='2']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='6'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='7'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='8'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='9'][.='0']",
        "//lst[@name='facet_ranges']/lst[@name='duration_i1']/lst[@name='counts']/int[@name='10'][.='2']"
    );
  }

  @Test
  public void testSimpleGroupedFacets() throws Exception {
    // for the purposes of our test data, it shouldn't matter 
    // if we use facet.limit -100, -1, or 100 ...
    // our set of values is small enough either way
    testSimpleGroupedFacets("100");
    testSimpleGroupedFacets("-100");
    testSimpleGroupedFacets("-5");
    testSimpleGroupedFacets("-1");
  }
  
  private void testSimpleGroupedFacets(String facetLimit) throws Exception {
    assertQ(
        "Return 5 docs with id range 1937 till 1940",
         req("id:[2000 TO 2004]"),
        "*[count(//doc)=5]"
    );
    assertQ(
        "Return two facet counts for field airport_a",
         req(
             "q", "*:*",
             "fq", "id:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.limit", facetLimit, 
             "facet.field", "airport_s1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=2]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']",
        "//lst[@name='airport_s1']/int[@name='dus'][.='1']"
    );
    assertQ(
        "Return one facet count for field airport_a using facet.offset",
         req(
             "q", "*:*",
             "fq", "id:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.offset", "1", 
             "facet.limit", facetLimit, 
             "facet.field", "airport_s1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=1]",
        "//lst[@name='airport_s1']/int[@name='dus'][.='1']"
    );
    assertQ(
        "Return two facet counts for field airport_a with fq",
         req(
             "q", "*:*",
             "fq", "id:[2000 TO 2004]",
             "fq", "duration_i1:5",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.limit", facetLimit, 
             "facet.field", "airport_s1"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=2]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']",
        "//lst[@name='airport_s1']/int[@name='dus'][.='0']"
    );
    assertQ(
        "Return one facet count for field airport_s1 with prefix a",
         req(
             "q", "*:*",
             "fq", "id:[2000 TO 2004]",
             "group", "true",
             "group.facet", "true",
             "group.field", "hotel_s1",
             "facet", "true",
             "facet.field", "airport_s1",
             "facet.limit", facetLimit, 
             "facet.prefix", "a"
         ),
        "//lst[@name='facet_fields']/lst[@name='airport_s1']",
        "*[count(//lst[@name='airport_s1']/int)=1]",
        "//lst[@name='airport_s1']/int[@name='ams'][.='2']"
    );
    
    try {
      h.query(
           req(
               "q", "*:*",
               "fq", "id:[2000 TO 2004]",
               "group.facet", "true",
               "facet", "true",
               "facet.field", "airport_s1",
               "facet.prefix", "a"
           )
      );
      fail("Exception should have been thrown");
    } catch (SolrException e) {
      assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    }
  }

  @Test
  public void testEmptyFacetCounts() throws Exception {
    doEmptyFacetCounts();
  }

  // static so we can try both with and without an empty index
  static void doEmptyFacetCounts() throws Exception {
    doEmptyFacetCounts("empty_t", new String[]{null, "myprefix",""});
    doEmptyFacetCounts("empty_i", new String[]{null});
    doEmptyFacetCounts("empty_f", new String[]{null});
    doEmptyFacetCounts("empty_s", new String[]{null, "myprefix",""});
    doEmptyFacetCounts("empty_d", new String[]{null});
  }
  
  static void doEmptyFacetCounts(String field, String[] prefixes) throws Exception {
    SchemaField sf = h.getCore().getLatestSchema().getField(field);

    String response = JQ(req("q", "*:*"));
    Map rsp = (Map) ObjectBuilder.fromJSON(response);
    Long numFound  = (Long)(((Map)rsp.get("response")).get("numFound"));

    ModifiableSolrParams params = params("q","*:*", "rows","0", "facet","true", "facet.field","{!key=myalias}"+field);
    
    String[] methods = {null, "fc","enum","fcs"};
    if (sf.multiValued() || sf.getType().multiValuedFieldCache()) {
      methods = new String[]{null, "fc","enum"};
    }

    prefixes = prefixes==null ? new String[]{null} : prefixes;


    for (String method : methods) {
      if (method == null) {
        params.remove("facet.method");
      } else {
        params.set("facet.method", method);
      }
      for (String prefix : prefixes) {
        if (prefix == null) {
          params.remove("facet.prefix");
        } else {
          params.set("facet.prefix", prefix);
        }

        for (String missing : new String[] {null, "true"}) {
          if (missing == null) {
            params.remove("facet.missing");
          } else {
            params.set("facet.missing", missing);
          }
          
          String expected = missing==null ? "[]" : "[null," + numFound + "]";
          
          assertJQ(req(params),
              "/facet_counts/facet_fields/myalias==" + expected);
        }
      }
    }
  }


  @Test
  public void testSimpleFacetCounts() {
 
    assertQ("standard request handler returns all matches",
            req("id:[42 TO 47]"),
            "*[count(//doc)=6]"
            );
 
    assertQ("filter results using fq",
            req("q","id:[42 TO 46]",
                "fq", "id:[43 TO 47]"),
            "*[count(//doc)=4]"
            );
    
    assertQ("don't filter results using blank fq",
            req("q","id:[42 TO 46]",
                "fq", " "),
            "*[count(//doc)=5]"
            );
     
    assertQ("filter results using multiple fq params",
            req("q","id:[42 TO 46]",
                "fq", "trait_s:Obnoxious",
                "fq", "id:[43 TO 47]"),
            "*[count(//doc)=1]"
            );
 
    assertQ("check counts for facet queries",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.query", "trait_s:Obnoxious"
                ,"facet.query", "id:[42 TO 45]"
                ,"facet.query", "id:[43 TO 47]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=6]"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='trait_s:Obnoxious'][.='2']"
            ,"//lst[@name='facet_queries']/int[@name='id:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id:[43 TO 47]'][.='5']"
 
            ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
            ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
            );

    assertQ("check multi-select facets with naming",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.query", "{!ex=1}trait_s:Obnoxious"
                ,"facet.query", "{!ex=2 key=foo}id:[42 TO 45]"    // tag=2 same as 1
                ,"facet.query", "{!ex=3,4 key=bar}id:[43 TO 47]"  // tag=3,4 don't exist
                ,"facet.field", "{!ex=3,1}trait_s"                // 3,1 same as 1
                ,"fq", "{!tag=1,2}id:47"                          // tagged as 1 and 2
                )
            ,"*[count(//doc)=1]"

            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='{!ex=1}trait_s:Obnoxious'][.='2']"
            ,"//lst[@name='facet_queries']/int[@name='foo'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='bar'][.='1']"

            ,"//lst[@name='facet_counts']/lst[@name='facet_fields']"
            ,"//lst[@name='facet_fields']/lst[@name='trait_s']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='1']"
            );

    // test excluding main query
    assertQ(req("q", "{!tag=main}id:43"
                 ,"facet", "true"
                 ,"facet.query", "{!key=foo}id:42"
                 ,"facet.query", "{!ex=main key=bar}id:42"    // only matches when we exclude main query
                 )
             ,"//lst[@name='facet_queries']/int[@name='foo'][.='0']"
             ,"//lst[@name='facet_queries']/int[@name='bar'][.='1']"
             );

    assertQ("check counts for applied facet queries using filtering (fq)",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.query", "id:[42 TO 45]"
                ,"facet.query", "id:[43 TO 47]"
                )
            ,"*[count(//doc)=4]"
            ,"//lst[@name='facet_counts']/lst[@name='facet_queries']"
            ,"//lst[@name='facet_queries']/int[@name='id:[42 TO 45]'][.='4']"
            ,"//lst[@name='facet_queries']/int[@name='id:[43 TO 47]'][.='3']"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );
 
    assertQ("check counts with facet.zero=false&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.zeros", "false"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

    assertQ("check counts with facet.mincount=1&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "1"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"
            );

    assertQ("check counts with facet.mincount=2&facet.missing=true using fq",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.mincount", "2"
                ,"f.trait_s.facet.missing", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                )
            ,"*[count(//doc)=4]"
            ,"*[count(//lst[@name='trait_s']/int)=2]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[not(@name)][.='1']"               
            );

    assertQ("check sorted paging",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","4"
                )
            ,"*[count(//lst[@name='trait_s']/int)=4]"
            ,"//lst[@name='trait_s']/int[@name='Tool'][.='2']"
            ,"//lst[@name='trait_s']/int[@name='Obnoxious'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Chauvinist'][.='1']"
            ,"//lst[@name='trait_s']/int[@name='Pig'][.='0']"
            );

    // check that the default sort is by count
    assertQ("check sorted paging",
            req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","3"
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Tool'][.='2']"
            ,"//int[2][@name='Chauvinist'][.='1']"
            ,"//int[3][@name='Obnoxious'][.='1']"
            );

    //
    // check that legacy facet.sort=true/false works
    //
    assertQ(req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","0"
                ,"facet.offset","0"
                ,"facet.limit","3"
                ,"facet.sort","true"  // true means sort-by-count
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Tool'][.='2']"
            ,"//int[2][@name='Chauvinist'][.='1']"
            ,"//int[3][@name='Obnoxious'][.='1']"
            );

     assertQ(req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "trait_s"
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","3"
                ,"facet.sort","false"  // false means sort by index order
                )
            ,"*[count(//lst[@name='trait_s']/int)=3]"
            ,"//int[1][@name='Chauvinist'][.='1']"
            ,"//int[2][@name='Obnoxious'][.='1']"
            ,"//int[3][@name='Tool'][.='2']"
            );


     assertQ(req("q", "id:[42 TO 47]"
                ,"facet", "true"
                ,"facet.method","fc"
                ,"fq", "id:[42 TO 45]"
                ,"facet.field", "zerolen_s"
                )
            ,"*[count(//lst[@name='zerolen_s']/int)=1]"
     );

    assertQ("a facet.query that analyzes to no query shoud not NPE",
        req("q", "*:*",
            "facet", "true",
            "facet.query", "{!field key=k f=lengthfilt}a"),//2 char minimum
        "//lst[@name='facet_queries']/int[@name='k'][.='0']"
    );
  }

  public static void indexDateFacets() {
    final String i = "id";
    final String f = "bday";
    final String ff = "a_tdt";
    final String ooo = "00:00:00.000Z";
    final String xxx = "15:15:15.155Z";

    add_doc(i, "201",  f, "1976-07-04T12:08:56.235Z", ff, "1900-01-01T"+ooo);
    add_doc(i, "202",  f, "1976-07-05T00:00:00.000Z", ff, "1976-07-01T"+ooo);
    add_doc(i, "203",  f, "1976-07-15T00:07:67.890Z", ff, "1976-07-04T"+ooo);
    add_doc(i, "204",  f, "1976-07-21T00:07:67.890Z", ff, "1976-07-05T"+ooo);
    add_doc(i, "205",  f, "1976-07-13T12:12:25.255Z", ff, "1976-07-05T"+xxx);
    add_doc(i, "206",  f, "1976-07-03T17:01:23.456Z", ff, "1976-07-07T"+ooo);
    add_doc(i, "207",  f, "1976-07-12T12:12:25.255Z", ff, "1976-07-13T"+ooo);
    add_doc(i, "208",  f, "1976-07-15T15:15:15.155Z", ff, "1976-07-13T"+xxx);
    add_doc(i, "209",  f, "1907-07-12T13:13:23.235Z", ff, "1976-07-15T"+xxx);
    add_doc(i, "2010", f, "1976-07-03T11:02:45.678Z", ff, "2000-01-01T"+ooo);
    add_doc(i, "2011", f, "1907-07-12T12:12:25.255Z");
    add_doc(i, "2012", f, "2007-07-30T07:07:07.070Z");
    add_doc(i, "2013", f, "1976-07-30T22:22:22.222Z");
    add_doc(i, "2014", f, "1976-07-05T22:22:22.222Z");
  }

  @Test
  public void testTrieDateFacets() {
    helpTestDateFacets("bday", false);
  }

  @Test
  public void testTrieDateRangeFacets() {
    helpTestDateFacets("bday", true);
  }

  private void helpTestDateFacets(final String fieldName, 
                                  final boolean rangeMode) {
    final String p = rangeMode ? "facet.range" : "facet.date";
    final String b = rangeMode ? "facet_ranges" : "facet_dates";
    final String f = fieldName;
    final String c = (rangeMode ? "/lst[@name='counts']" : "");
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + (rangeMode ? "/../" : "");

    
    // date faceting defaults to include both endpoints, 
    // range faceting defaults to including only lower
    // doc exists with value @ 00:00:00.000 on July5
    final String jul4 = rangeMode ? "[.='1'  ]" : "[.='2'  ]";

    assertQ("check counts for month of facet by day",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 31 : 34)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-16T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-17T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-18T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-19T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-21T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-22T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-23T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-24T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-25T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-26T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-27T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-28T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-29T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-30T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-31T00:00:00Z'][.='0']"
            
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            
            );

    assertQ("check counts for month of facet by day with global mincount = 1",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,"facet.mincount", "1"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 8 : 11)+"]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-21T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-30T00:00:00Z'][.='1'  ]"
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            );

    assertQ("check counts for month of facet by day with field mincount = 1",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-01T00:00:00.000Z+1MONTH"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,"f." + f + ".facet.mincount", "2"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 3 : 7)+"]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+(rangeMode ? "" : "/int[@name='1976-07-04T00:00:00Z']" +jul4)
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='2'  ]"
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='11']"
            );

    assertQ("check before is not inclusive of upper bound by default",
            req("q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-05T00:00:00.000Z"
                ,p+".end",    "1976-07-07T00:00:00.000Z"
                ,p+".gap",    "+1DAY"
                ,p+".other",  "all"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 2 : 5)+"]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            
            ,meta+"/int[@name='before' ][.='5']"
            );
    assertQ("check after is not inclusive of lower bound by default (for dates)",
            req("q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-03T00:00:00.000Z"
                ,p+".end",    "1976-07-05T00:00:00.000Z"
                ,p+".gap",    "+1DAY"
                ,p+".other",  "all"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 2 : 5)+"]"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z']" + jul4
            
            ,meta+"/int[@name='after' ][.='"+(rangeMode ? 9 : 8)+"']"
            );
            

    assertQ("check hardend=false",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-01T00:00:00.000Z"
                ,p+".end",    "1976-07-13T00:00:00.000Z"
                ,p+".gap",    "+5DAYS"
                ,p+".other",  "all"
                ,p+".hardend","false"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 3 : 6)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='4'  ]"
            
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='3']"
            ,meta+"/int[@name='between'][.='9']"
            );

    assertQ("check hardend=true",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start",  "1976-07-01T00:00:00.000Z"
                ,p+".end",    "1976-07-13T00:00:00.000Z"
                ,p+".gap",    "+5DAYS"
                ,p+".other",  "all"
                ,p+".hardend","true"
                )
            ,"*[count("+pre+"/int)="+(rangeMode ? 3 : 6)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='5'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0'  ]"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='1'  ]"
            
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='6']"
            ,meta+"/int[@name='between'][.='6']"
            );
    
  }

  @Test
  public void testTrieDateFacetsWithIncludeOption() {
    helpTestDateFacetsWithIncludeOption("a_tdt", false);
  }

  @Test
  public void testTrieDateRangeFacetsWithIncludeOption() {
    helpTestDateFacetsWithIncludeOption("a_tdt", true);
  }

  /** similar to helpTestDateFacets, but for differnet fields with test data 
      exactly on on boundary marks */
  private void helpTestDateFacetsWithIncludeOption(final String fieldName,
                                                   final boolean rangeMode) {
    final String p = rangeMode ? "facet.range" : "facet.date";
    final String b = rangeMode ? "facet_ranges" : "facet_dates";
    final String f = fieldName;
    final String c = (rangeMode ? "/lst[@name='counts']" : "");
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + (rangeMode ? "/../" : "");

    assertQ("checking counts for lower",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='7']"
            );

    assertQ("checking counts for lower & upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "upper"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-16T00:00:00.000Z"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                ,p+".include", "edge"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='8']"
            );

    assertQ("checking counts for upper & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "upper"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)="+(rangeMode ? 12 : 15)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ("checking counts for lower & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "edge"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)="+(rangeMode ? 12 : 15)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='3']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ("checking counts for lower & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)="+(rangeMode ? 12 : 15)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='0']"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ("checking counts for lower & edge & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                ,p+".include", "edge"
                ,p+".include", "outer"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)="+(rangeMode ? 12 : 15)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ("checking counts for all",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,p, f
                ,p+".start", "1976-07-01T00:00:00.000Z"
                ,p+".end",   "1976-07-13T00:00:00.000Z" // smaller now
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "all"
                )
            // 12 days + pre+post+inner = 15
            ,"*[count("+pre+"/int)="+(rangeMode ? 12 : 15)+"]"
            ,pre+"/int[@name='1976-07-01T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-02T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-05T00:00:00Z'][.='2'  ]"
            ,pre+"/int[@name='1976-07-06T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T00:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-08T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T00:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T00:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='4']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }

  @Test
  public void testDateFacetsWithTz() {
    for (boolean rangeType : new boolean[] { true, false }) {
      helpTestDateFacetsWithTz("a_tdt", rangeType);
    }
  }

  private void helpTestDateFacetsWithTz(final String fieldName,
                                        final boolean rangeMode) {
    final String p = rangeMode ? "facet.range" : "facet.date";
    final String b = rangeMode ? "facet_ranges" : "facet_dates";
    final String f = fieldName;
    final String c = (rangeMode ? "/lst[@name='counts']" : "");
    final String pre = "//lst[@name='"+b+"']/lst[@name='"+f+"']" + c;
    final String meta = pre + (rangeMode ? "/../" : "");

    final String TZ = "America/Los_Angeles";
    assumeTrue("Test requires JVM to know about about TZ: " + TZ,
               TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(TZ)); 

    assertQ("checking facet counts for fixed now, using TZ: " + TZ,
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"NOW", "205078333000" // 1976-07-01T14:12:13.000Z
                ,"TZ", TZ
                ,p, f
                ,p+".start", "NOW/MONTH"
                ,p+".end",   "NOW/MONTH+15DAYS"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='1976-07-01T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-02T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-03T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-04T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-05T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-06T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-07T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-08T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-09T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-10T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-11T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-12T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-13T07:00:00Z'][.='1'  ]"
            ,pre+"/int[@name='1976-07-14T07:00:00Z'][.='0']"
            ,pre+"/int[@name='1976-07-15T07:00:00Z'][.='1'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='7']"
            );

    // NOTE: the counts should all be zero, what we really care about
    // is that the computed lower bounds take into account DST change
    assertQ("checking facet counts arround DST change for TZ: " + TZ,
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"NOW", "1288606136000" // 2010-11-01T10:08:56.235Z
                ,"TZ", TZ
                ,p, f
                ,p+".start", "NOW/MONTH"
                ,p+".end",   "NOW/MONTH+15DAYS"
                ,p+".gap",   "+1DAY"
                ,p+".other", "all"
                ,p+".include", "lower"
                )
            // 15 days + pre+post+inner = 18
            ,"*[count("+pre+"/int)="+(rangeMode ? 15 : 18)+"]"
            ,pre+"/int[@name='2010-11-01T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-02T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-03T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-04T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-05T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-06T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-07T07:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-08T08:00:00Z'][.='0']" // BOOM!
            ,pre+"/int[@name='2010-11-09T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-10T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-11T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-12T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-13T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-14T08:00:00Z'][.='0']"
            ,pre+"/int[@name='2010-11-15T08:00:00Z'][.='0']"
            );
    
  }

  @Test
  public void testNumericRangeFacetsTrieFloat() {
    helpTestFractionalNumberRangeFacets("range_facet_f");
  }
  @Test
  public void testNumericRangeFacetsTrieDouble() {
    helpTestFractionalNumberRangeFacets("range_facet_d");
  }

  @Test
  public void testNumericRangeFacetsOverflowTrieDouble() {
    helpTestNumericRangeFacetsDoubleOverflow("range_facet_d");
  }

  private void helpTestNumericRangeFacetsDoubleOverflow(final String fieldName) {
    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    String start = "0.0";
    String gap = (new Double( (double)Float.MAX_VALUE )).toString();
    String end = (new Double( ((double)Float.MAX_VALUE) * 3D )).toString();
    String mid = (new Double( ((double)Float.MAX_VALUE) * 2D )).toString();

    assertQ(f+": checking counts for lower",
            req( "q", "id:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", start
                ,"facet.range.end",   end
                ,"facet.range.gap",   gap
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='"+start+"'][.='6'  ]"
            ,pre+"/int[@name='"+mid+"'][.='0'  ]"
            //
            ,meta+"/double[@name='end' ][.='"+end+"']"
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }
   private void helpTestFractionalNumberRangeFacets(final String fieldName) {

    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    assertQ(f+": checking counts for lower",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='1'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='1'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for lower & upper",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "10"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=4]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ": checking counts for upper & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "20"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "edge"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='1'  ]"
            ,pre+"/int[@name='40.0'][.='0'  ]"
            //
            ,meta+"/int[@name='before' ][.='1']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for upper & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "10"
                ,"facet.range.end",   "30"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='10.0'][.='2'  ]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='2']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & edge",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "10"
                ,"facet.range.end",   "30"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "edge"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='10.0'][.='1'  ]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "20"
                ,"facet.range.end",   "40"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for lower & edge & outer",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "20"
                ,"facet.range.end",   "35.3"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.hardend", "true"
                ,"facet.range.include", "lower"
                ,"facet.range.include", "edge"
                ,"facet.range.include", "outer"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='3'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );

    assertQ(f + ": checking counts for include all",
            req( "q", "*:*"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "20"
                ,"facet.range.end",   "35.3"
                ,"facet.range.gap",   "10"
                ,"facet.range.other", "all"
                ,"facet.range.hardend", "true"
                ,"facet.range.include", "all"
                )
            ,"*[count("+pre+"/int)=2]"
            ,pre+"/int[@name='20.0'][.='4'  ]"
            ,pre+"/int[@name='30.0'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='2']"
            ,meta+"/int[@name='after'  ][.='1']"
            ,meta+"/int[@name='between'][.='5']"
            );
  }

  @Test
  public void testNumericRangeFacetsTrieInt() {
    helpTestWholeNumberRangeFacets("id");
  }
  @Test
  public void testNumericRangeFacetsTrieLong() {
    helpTestWholeNumberRangeFacets("range_facet_l");
  }

  @Test
  public void testNumericRangeFacetsOverflowTrieLong() {
    helpTestNumericRangeFacetsLongOverflow("range_facet_l");
  }

  private void helpTestNumericRangeFacetsLongOverflow(final String fieldName) {
    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    String start = "0";
    String gap = (new Long( (long)Integer.MAX_VALUE )).toString();
    String end = (new Long( ((long)Integer.MAX_VALUE) * 3L )).toString();
    String mid = (new Long( ((long)Integer.MAX_VALUE) * 2L )).toString();

    assertQ(f+": checking counts for lower",
            req( "q", "id:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", start
                ,"facet.range.end",   end
                ,"facet.range.gap",   gap
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='"+start+"'][.='6'  ]"
            ,pre+"/int[@name='"+mid+"'][.='0'  ]"
            //
            ,meta+"/long[@name='end'   ][.='"+end+"']"
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
  }
  private void helpTestWholeNumberRangeFacets(final String fieldName) {

    // the float test covers a lot of the weird edge cases
    // here we just need some basic sanity checking of the parsing

    final String f = fieldName;
    final String pre = "//lst[@name='facet_ranges']/lst[@name='"+f+"']/lst[@name='counts']";
    final String meta = pre + "/../";

    assertQ(f+": checking counts for lower",
            req( "q", "id:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "35"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "5"
                ,"facet.range.other", "all"
                ,"facet.range.include", "lower"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='35'][.='0'  ]"
            ,pre+"/int[@name='40'][.='3'  ]"
            ,pre+"/int[@name='45'][.='3'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );

    assertQ(f + ":checking counts for upper",
            req( "q", "id:[30 TO 60]"
                ,"rows", "0"
                ,"facet", "true"
                ,"facet.range", f
                ,"facet.range.start", "35"
                ,"facet.range.end",   "50"
                ,"facet.range.gap",   "5"
                ,"facet.range.other", "all"
                ,"facet.range.include", "upper"
                )
            ,"*[count("+pre+"/int)=3]"
            ,pre+"/int[@name='35'][.='0'  ]"
            ,pre+"/int[@name='40'][.='4'  ]"
            ,pre+"/int[@name='45'][.='2'  ]"
            //
            ,meta+"/int[@name='before' ][.='0']"
            ,meta+"/int[@name='after'  ][.='0']"
            ,meta+"/int[@name='between'][.='6']"
            );
    
  }

  static void indexFacetSingleValued() {
    indexFacets("40","t_s1");
  }

  @Test
  public void testFacetSingleValued() {
    doFacets("t_s1");
  }
  @Test
  public void testFacetSingleValuedFcs() {
    doFacets("t_s1","facet.method","fcs");
  }

  static void indexFacets(String idPrefix, String f) {
    add_doc("id", idPrefix+"1",  f, "A");
    add_doc("id", idPrefix+"2",  f, "B");
    add_doc("id", idPrefix+"3",  f, "C");
    add_doc("id", idPrefix+"4",  f, "C");
    add_doc("id", idPrefix+"5",  f, "D");
    add_doc("id", idPrefix+"6",  f, "E");
    add_doc("id", idPrefix+"7",  f, "E");
    add_doc("id", idPrefix+"8",  f, "E");
    add_doc("id", idPrefix+"9",  f, "F");
    add_doc("id", idPrefix+"10", f, "G");
    add_doc("id", idPrefix+"11", f, "G");
    add_doc("id", idPrefix+"12", f, "G");
    add_doc("id", idPrefix+"13", f, "G");
    add_doc("id", idPrefix+"14", f, "G");
  }

  public void doFacets(String f, String... params) {
    String pre = "//lst[@name='"+f+"']";
    String notc = "id:[* TO *] -"+f+":C";


    assertQ("check counts for unlimited facet",
            req(params, "q", "id:[* TO *]", "indent","true"
                ,"facet", "true"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[@name='G'][.='5']"
            ,pre+"/int[@name='E'][.='3']"
            ,pre+"/int[@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for facet with generous limit",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "100"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=7]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='C'][.='2']"

            ,pre+"/int[@name='A'][.='1']"
            ,pre+"/int[@name='B'][.='1']"
            ,pre+"/int[@name='D'][.='1']"
            ,pre+"/int[@name='F'][.='1']"
            );

    assertQ("check counts for limited facet",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.limit", "2"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"

            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            );

   assertQ("check offset",
            req(params, "q", "id:[* TO *]"
                ,"facet", "true"
                ,"facet.offset", "1"
                ,"facet.limit", "1"
                ,"facet.field", f
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"

            ,pre+"/int[1][@name='E'][.='3']"
            );

    assertQ("test sorted facet paging with zero (don't count in limit)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='G'][.='5']"
            ,pre+"/int[2][@name='E'][.='3']"
            ,pre+"/int[3][@name='A'][.='1']"
            ,pre+"/int[4][@name='B'][.='1']"
            ,pre+"/int[5][@name='D'][.='1']"
            ,pre+"/int[6][@name='F'][.='1']"
            );

    assertQ("test sorted facet paging with zero (test offset correctness)",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","count"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='D'][.='1']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","0"
                ,"facet.limit","6"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=6]"
            ,pre+"/int[1][@name='A'][.='1']"
            ,pre+"/int[2][@name='B'][.='1']"
            ,pre+"/int[3][@name='D'][.='1']"
            ,pre+"/int[4][@name='E'][.='3']"
            ,pre+"/int[5][@name='F'][.='1']"
            ,pre+"/int[6][@name='G'][.='5']"
            );

   assertQ("test facet unsorted paging",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","1"
                ,"facet.offset","3"
                ,"facet.limit","2"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='E'][.='3']"
            ,pre+"/int[2][@name='F'][.='1']"
            );

    assertQ("test facet unsorted paging, mincount=2",
            req(params, "q", "id:[* TO *]"
                ,"fq",notc
                ,"facet", "true"
                ,"facet.field", f
                ,"facet.mincount","2"
                ,"facet.offset","1"
                ,"facet.limit","2"
                ,"facet.sort","index"
                )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='G'][.='5']"
            );
  }


  static void indexFacetPrefixMultiValued() {
    indexFacetPrefix("50","t_s");
  }

  @Test
  public void testFacetPrefixMultiValued() {
    doFacetPrefix("t_s", null, "facet.method","enum");
    doFacetPrefix("t_s", null, "facet.method", "enum", "facet.enum.cache.minDf", "3");
    doFacetPrefix("t_s", null, "facet.method", "enum", "facet.enum.cache.minDf", "100");
    doFacetPrefix("t_s", null, "facet.method", "fc");
  }

  static void indexFacetPrefixSingleValued() {
    indexFacetPrefix("60","tt_s1");
  }

  @Test
  public void testFacetPrefixSingleValued() {
    doFacetPrefix("tt_s1", null);
  }
  @Test
  public void testFacetPrefixSingleValuedFcs() {
    doFacetPrefix("tt_s1", null, "facet.method","fcs");
    doFacetPrefix("tt_s1", "{!threads=0}", "facet.method","fcs");   // direct execution
    doFacetPrefix("tt_s1", "{!threads=-1}", "facet.method","fcs");  // default / unlimited threads
    doFacetPrefix("tt_s1", "{!threads=2}", "facet.method","fcs");   // specific number of threads
  }


  static void indexFacetPrefix(String idPrefix, String f) {
    add_doc("id", idPrefix+"1",  f, "AAA");
    add_doc("id", idPrefix+"2",  f, "B");
    add_doc("id", idPrefix+"3",  f, "BB");
    add_doc("id", idPrefix+"4",  f, "BB");
    add_doc("id", idPrefix+"5",  f, "BBB");
    add_doc("id", idPrefix+"6",  f, "BBB");
    add_doc("id", idPrefix+"7",  f, "BBB");
    add_doc("id", idPrefix+"8",  f, "CC");
    add_doc("id", idPrefix+"9",  f, "CC");
    add_doc("id", idPrefix+"10", f, "CCC");
    add_doc("id", idPrefix+"11", f, "CCC");
    add_doc("id", idPrefix+"12", f, "CCC");
    assertU(commit());
  }

  public void doFacetPrefix(String f, String local, String... params) {
    String indent="on";
    String pre = "//lst[@name='"+f+"']";
    String notc = "id:[* TO *] -"+f+":C";
    String lf = local==null ? f : local+f;


    assertQ("test facet.prefix middle, exact match first term",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='BBB'][.='3']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='B'][.='1']"
    );

    assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","index"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='BBB'][.='3']"
    );


     assertQ("test facet.prefix middle, exact match first term, unsorted",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","index"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=3]"
            ,pre+"/int[1][@name='B'][.='1']"
            ,pre+"/int[2][@name='BB'][.='2']"
            ,pre+"/int[3][@name='BBB'][.='3']"
    );


    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='BB'][.='2']"
            ,pre+"/int[2][@name='B'][.='1']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB'][.='2']"
    );

    assertQ("test facet.prefix middle, paging",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","1"
                    ,"facet.sort","count"
                    ,"facet.prefix","B"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='BB'][.='2']"
    );

    assertQ("test facet.prefix end, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","C"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC'][.='3']"
            ,pre+"/int[2][@name='CC'][.='2']"
    );

    assertQ("test facet.prefix end, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=2]"
            ,pre+"/int[1][@name='CCC'][.='3']"
            ,pre+"/int[2][@name='CC'][.='2']"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix past end",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","1"
                    ,"facet.limit","-1"
                    ,"facet.sort","count"
                    ,"facet.prefix","X"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix at start, exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AAA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );
    assertQ("test facet.prefix at Start, not exact match",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","AA"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=1]"
            ,pre+"/int[1][@name='AAA'][.='1']"
    );    
    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","0"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    assertQ("test facet.prefix before start",
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","0"
                    ,"facet.offset","2"
                    ,"facet.limit","100"
                    ,"facet.sort","count"
                    ,"facet.prefix","999"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );

    // test offset beyond what is collected internally in queue
    assertQ(
            req(params, "q", "id:[* TO *]"
                    ,"indent",indent
                    ,"facet","true"
                    ,"facet.field", lf
                    ,"facet.mincount","3"
                    ,"facet.offset","5"
                    ,"facet.limit","10"
                    ,"facet.sort","count"
                    ,"facet.prefix","CC"
            )
            ,"*[count(//lst[@name='facet_fields']/lst/int)=0]"
    );
  }

  /** 
   * kind of an absurd tests because if there is an inifnite loop, it 
   * would ver finish -- but at least it ensures that <i>if</i> one of 
   * these requests return, they return an error 
   */
  public void testRangeFacetInfiniteLoopDetection() {

    for (String field : new String[] {"foo_f", "foo_d", "foo_i"}) {
      assertQEx("no zero gap error: " + field,
                req("q", "*:*",
                    "facet", "true",
                    "facet.range", field,
                    "facet.range.start", "23",
                    "facet.range.gap", "0",
                    "facet.range.end", "100"),
                400);
    }
    String field = "foo_dt";
    for (String type : new String[]{"date", "range"}) {
      assertQEx("no zero gap error for facet." + type + ": " + field,
                req("q", "*:*",
                    "facet", "true",
                    "facet." + type, field,
                    "facet."+type+".start", "NOW",
                    "facet."+type+".gap", "+0DAYS",
                    "facet."+type+".end", "NOW+10DAY"),
                400);
    }
    field = "foo_f";
    assertQEx("no float underflow error: " + field,
              req("q", "*:*",
                  "facet", "true",
                  "facet.range", field,
                  "facet.range.start", "100000000000",
                  "facet.range.end", "100000086200",
                  "facet.range.gap", "2160"),
              400);

    field = "foo_d";
    assertQEx("no double underflow error: " + field,
              req("q", "*:*",
                  "facet", "true",
                  "facet.range", field,
                  "facet.range.start", "9900000000000",
                  "facet.range.end", "9900000086200",
                  "facet.range.gap", "0.0003"),
              400);
  }

}
