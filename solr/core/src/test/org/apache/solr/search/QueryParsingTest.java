package org.apache.solr.search;
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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 *
 **/
public class QueryParsingTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  /**
   * Test that the main QParserPlugins people are likely to use
   * as defaults fail with a consistent exception when the query string 
   * is either empty or null.
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-435">SOLR-435</a>
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-2001">SOLR-2001</a>
   */
  public void testQParserEmptyInput() throws Exception {
    
    SolrQueryRequest req = req();
    
    final String[] parsersTested = new String[] {
      OldLuceneQParserPlugin.NAME,
      LuceneQParserPlugin.NAME,
      DisMaxQParserPlugin.NAME,
      ExtendedDismaxQParserPlugin.NAME
    };

    for (String defType : parsersTested) {
      for (String qstr : new String[] {null, ""}) {
        QParser parser = null;
        try {
          parser = QParser.getParser(qstr, defType, req);
        } catch (Exception e) {
          throw new RuntimeException("getParser excep using defType=" + 
                                     defType + " with qstr="+qstr, e);
        }
        
        Query q = parser.parse();
        assertNull("expected no query",q);
      }
    }
  }
  
  @Test
  public void testSort() throws Exception {
    Sort sort;
    SolrQueryRequest req = req();

    sort = QueryParsing.parseSort("score desc", req);
    assertNull("sort", sort);//only 1 thing in the list, no Sort specified

    // SOLR-4458 - using different case variations of asc and desc
    sort = QueryParsing.parseSort("score aSc", req);
    SortField[] flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.SCORE);
    assertTrue(flds[0].getReverse());

    sort = QueryParsing.parseSort("weight dEsC", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    sort = QueryParsing.parseSort("weight desc,bday ASC", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    //order aliases
    sort = QueryParsing.parseSort("weight top,bday asc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);
    sort = QueryParsing.parseSort("weight top,bday bottom", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[0].getReverse(), true);
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getReverse(), false);

    //test weird spacing
    sort = QueryParsing.parseSort("weight         DESC,            bday         asc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");
    assertEquals(flds[1].getField(), "bday");
    assertEquals(flds[1].getType(), SortField.Type.LONG);
    //handles trailing commas
    sort = QueryParsing.parseSort("weight desc,", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");

    //test functions
    sort = QueryParsing.parseSort("pow(weight, 2) desc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2))");
    
    //test functions (more deep)
    sort = QueryParsing.parseSort("sum(product(r_f1,sum(d_f1,t_f1,1.0)),a_f1) asc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    assertEquals(flds[0].getField(), "sum(product(float(r_f1),sum(float(d_f1),float(t_f1),const(1.0))),float(a_f1))");

    sort = QueryParsing.parseSort("pow(weight,                 2.0)         desc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2.0))");


    sort = QueryParsing.parseSort("pow(weight, 2.0) desc, weight    desc,   bday    asc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);

    //Not thrilled about the fragility of string matching here, but...
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "pow(float(weight),const(2.0))");

    assertEquals(flds[1].getType(), SortField.Type.FLOAT);
    assertEquals(flds[1].getField(), "weight");
    assertEquals(flds[2].getField(), "bday");
    assertEquals(flds[2].getType(), SortField.Type.LONG);
    
    //handles trailing commas
    sort = QueryParsing.parseSort("weight desc,", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.FLOAT);
    assertEquals(flds[0].getField(), "weight");

    //Test literals in functions
    sort = QueryParsing.parseSort("strdist(foo_s1, \"junk\", jw) desc", req);
    flds = sort.getSort();
    assertEquals(flds[0].getType(), SortField.Type.REWRITEABLE);
    //the value sources get wrapped, so the out field is different than the input
    assertEquals(flds[0].getField(), "strdist(str(foo_s1),literal(junk), dist=org.apache.lucene.search.spell.JaroWinklerDistance)");

    sort = QueryParsing.parseSort("", req);
    assertNull(sort);

    req.close();
  }

  @Test
  public void testBad() throws Exception {
    Sort sort;
    SolrQueryRequest req = req();

    //test some bad vals
    try {
      sort = QueryParsing.parseSort("weight, desc", req);
      assertTrue(false);
    } catch (SolrException e) {
      //expected
    }
    try {
      sort = QueryParsing.parseSort("w", req);
      assertTrue(false);
    } catch (SolrException e) {
      //expected
    }
    try {
      sort = QueryParsing.parseSort("weight desc, bday", req);
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad number of commas
      sort = QueryParsing.parseSort("pow(weight,,2) desc, bday asc", req);
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad function
      sort = QueryParsing.parseSort("pow() desc, bday asc", req);
      assertTrue(false);
    } catch (SolrException e) {
    }

    try {
      //bad number of parens
      sort = QueryParsing.parseSort("pow((weight,2) desc, bday asc", req);
      assertTrue(false);
    } catch (SolrException e) {
    }

    req.close();
  }

  public void testLiteralFunction() throws Exception {
    
    final String NAME = FunctionQParserPlugin.NAME;

    SolrQueryRequest req = req("variable", "foobar");
    
    assertNotNull(QParser.getParser
                  ("literal('a value')",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal('a value')",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal(\"a value\")",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("literal($variable)",
                   NAME, req).getQuery());
    assertNotNull(QParser.getParser
                  ("strdist(\"a value\",literal('a value'),edit)",
                   NAME, req).getQuery());
  }
}
