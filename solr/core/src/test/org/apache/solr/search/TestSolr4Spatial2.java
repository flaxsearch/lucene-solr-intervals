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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

//Unlike TestSolr4Spatial, not parameterized / not generic.
//We exclude Codecs that don't support DocValues (though not sure if this list is quite right)
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Appending", "Lucene40", "Lucene41"})
public class TestSolr4Spatial2 extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-spatial.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
  }

  @Test
  public void testBBox() throws Exception {
    String fieldName = "bbox";
    assertU(adoc("id", "0"));//nothing
    assertU(adoc("id", "1", fieldName, "ENVELOPE(-10, 20, 15, 10)"));
    assertU(adoc("id", "2", fieldName, "ENVELOPE(22, 22, 10, 10)"));//pt
    assertU(commit());

    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=overlapRatio " +
                "queryTargetProportion=0.25}" +
                "Intersects(ENVELOPE(10,25,12,10))",
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='2'",
        "/response/docs/[0]/score==0.75]",
        "/response/docs/[1]/id=='1'",
        "/response/docs/[1]/score==0.26666668]",
        "/response/docs/[2]/id=='0'",
        "/response/docs/[2]/score==0.0"
        );

    //minSideLength with point query
    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=overlapRatio " +
                "queryTargetProportion=0.5 minSideLength=1}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'",
        "/response/docs/[0]/score==0.50333333]"//just over 0.5
    );

    //area2D
    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=area2D}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'" ,
        "/response/docs/[0]/score==" + (30f * 5f) + "]"//150
    );
    //area (not 2D)
    assertJQ(req("q", "{!field f="+fieldName+" filter=false score=area}" +
                "Intersects(ENVELOPE(0,0,12,12))",//pt
            "fl", "id,score",
            "debug", "results"),//explain info
        "/response/docs/[0]/id=='1'" ,
        "/response/docs/[0]/score==" + 146.39793f + "]"//a bit less than 150
    );
  }

}
