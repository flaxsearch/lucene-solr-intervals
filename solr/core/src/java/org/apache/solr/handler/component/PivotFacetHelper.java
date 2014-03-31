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

package org.apache.solr.handler.component;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.index.Term;
import org.apache.solr.search.SyntaxError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @since solr 4.0
 */
public class PivotFacetHelper extends SimpleFacets
{

  protected int minMatch;

  public PivotFacetHelper(SolrQueryRequest req, DocSet docs, SolrParams params, ResponseBuilder rb) {
    super(req, docs, params, rb);
    minMatch = params.getInt( FacetParams.FACET_PIVOT_MINCOUNT, 1 );
  }

  public SimpleOrderedMap<List<NamedList<Object>>> process(String[] pivots) throws IOException {
    if (!rb.doFacets || pivots == null) 
      return null;

    SimpleOrderedMap<List<NamedList<Object>>> pivotResponse = new SimpleOrderedMap<>();
    for (String pivot : pivots) {
      //ex: pivot == "features,cat" or even "{!ex=mytag}features,cat"
      try {
        this.parseParams(FacetParams.FACET_PIVOT, pivot);
      } catch (SyntaxError e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, e);
      }
      pivot = facetValue;//facetValue potentially modified from parseParams()

      String[] fields = pivot.split(",");

      if( fields.length < 2 ) {
        throw new SolrException( ErrorCode.BAD_REQUEST,
            "Pivot Facet needs at least two fields: "+pivot );
      }

      String field = fields[0];
      String subField = fields[1];
      Deque<String> fnames = new LinkedList<>();
      for( int i=fields.length-1; i>1; i-- ) {
        fnames.push( fields[i] );
      }

      NamedList<Integer> superFacets = this.getTermCounts(field);

      //super.key usually == pivot unless local-param 'key' used
      pivotResponse.add(key, doPivots(superFacets, field, subField, fnames, docs));
    }
    return pivotResponse;
  }

  /**
   * Recursive function to do all the pivots
   */
  protected List<NamedList<Object>> doPivots(NamedList<Integer> superFacets,
                                             String field, String subField, Deque<String> fnames,
                                             DocSet docs) throws IOException
  {
    SolrIndexSearcher searcher = rb.req.getSearcher();
    // TODO: optimize to avoid converting to an external string and then having to convert back to internal below
    SchemaField sfield = searcher.getSchema().getField(field);
    FieldType ftype = sfield.getType();

    String nextField = fnames.poll();

    List<NamedList<Object>> values = new ArrayList<>( superFacets.size() );
    for (Map.Entry<String, Integer> kv : superFacets) {
      // Only sub-facet if parent facet has positive count - still may not be any values for the sub-field though
      if (kv.getValue() >= minMatch) {

        // may be null when using facet.missing
        final String fieldValue = kv.getKey(); 

        // don't reuse the same BytesRef each time since we will be 
        // constructing Term objects used in TermQueries that may be cached.
        BytesRef termval = null;

        SimpleOrderedMap<Object> pivot = new SimpleOrderedMap<>();
        pivot.add( "field", field );
        if (null == fieldValue) {
          pivot.add( "value", null );
        } else {
          termval = new BytesRef();
          ftype.readableToIndexed(fieldValue, termval);
          pivot.add( "value", ftype.toObject(sfield, termval) );
        }
        pivot.add( "count", kv.getValue() );
        
        if( subField == null ) {
          values.add( pivot );
        }
        else {
          DocSet subset = null;
          if ( null == termval ) {
            DocSet hasVal = searcher.getDocSet
              (new TermRangeQuery(field, null, null, false, false));
            subset = docs.andNot(hasVal);
          } else {
            Query query = new TermQuery(new Term(field, termval));
            subset = searcher.getDocSet(query, docs);
          }
          super.docs = subset;//used by getTermCounts()

          NamedList<Integer> nl = this.getTermCounts(subField);
          if (nl.size() >= minMatch) {
            pivot.add( "pivot", doPivots( nl, subField, nextField, fnames, subset) );
            values.add( pivot ); // only add response if there are some counts
          }
        }
      }
    }
    
    // put the field back on the list
    fnames.push( nextField );
    return values;
  }

// TODO: This is code from various patches to support distributed search.
//  Some parts may be helpful for whoever implements distributed search.
//
//  @Override
//  public int distributedProcess(ResponseBuilder rb) throws IOException {
//    if (!rb.doFacets) {
//      return ResponseBuilder.STAGE_DONE;
//    }
//
//    if (rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
//      SolrParams params = rb.req.getParams();
//      String[] pivots = params.getParams(FacetParams.FACET_PIVOT);
//      for ( ShardRequest sreq : rb.outgoing ) {
//        if (( sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS ) != 0
//            && sreq.shards != null && sreq.shards.length == 1 ) {
//          sreq.params.set( FacetParams.FACET, "true" );
//          sreq.params.set( FacetParams.FACET_PIVOT, pivots );
//          sreq.params.set( FacetParams.FACET_PIVOT_MINCOUNT, 1 ); // keep this at 1 regardless so that it accumulates everything
//            }
//      }
//    }
//    return ResponseBuilder.STAGE_DONE;
//  }
//
//  @Override
//  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
//    if (!rb.doFacets) return;
//
//
//    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FACETS)!=0) {
//      SimpleOrderedMap<List<NamedList<Object>>> tf = rb._pivots;
//      if ( null == tf ) {
//        tf = new SimpleOrderedMap<List<NamedList<Object>>>();
//        rb._pivots = tf;
//      }
//      for (ShardResponse srsp: sreq.responses) {
//        int shardNum = rb.getShardNum(srsp.getShard());
//
//        NamedList facet_counts = (NamedList)srsp.getSolrResponse().getResponse().get("facet_counts");
//
//        // handle facet trees from shards
//        SimpleOrderedMap<List<NamedList<Object>>> shard_pivots = 
//          (SimpleOrderedMap<List<NamedList<Object>>>)facet_counts.get( PIVOT_KEY );
//        
//        if ( shard_pivots != null ) {
//          for (int j=0; j< shard_pivots.size(); j++) {
//            // TODO -- accumulate the results from each shard
//            // The following code worked to accumulate facets for an previous 
//            // two level patch... it is here for reference till someone can upgrade
//            /**
//            String shard_tree_name = (String) shard_pivots.getName( j );
//            SimpleOrderedMap<NamedList> shard_tree = (SimpleOrderedMap<NamedList>)shard_pivots.getVal( j );
//            SimpleOrderedMap<NamedList> facet_tree = tf.get( shard_tree_name );
//            if ( null == facet_tree) { 
//              facet_tree = new SimpleOrderedMap<NamedList>(); 
//              tf.add( shard_tree_name, facet_tree );
//            }
//
//            for( int o = 0; o < shard_tree.size() ; o++ ) {
//              String shard_outer = (String) shard_tree.getName( o );
//              NamedList shard_innerList = (NamedList) shard_tree.getVal( o );
//              NamedList tree_innerList  = (NamedList) facet_tree.get( shard_outer );
//              if ( null == tree_innerList ) { 
//                tree_innerList = new NamedList();
//                facet_tree.add( shard_outer, tree_innerList );
//              }
//
//              for ( int i = 0 ; i < shard_innerList.size() ; i++ ) {
//                String shard_term = (String) shard_innerList.getName( i );
//                long shard_count  = ((Number) shard_innerList.getVal(i)).longValue();
//                int tree_idx      = tree_innerList.indexOf( shard_term, 0 );
//
//                if ( -1 == tree_idx ) {
//                  tree_innerList.add( shard_term, shard_count );
//                } else {
//                  long tree_count = ((Number) tree_innerList.getVal( tree_idx )).longValue();
//                  tree_innerList.setVal( tree_idx, shard_count + tree_count );
//                }
//              } // innerList loop
//            } // outer loop
//              **/
//          } // each tree loop
//        }
//      }
//    } 
//    return ;
//  }
//
//  @Override
//  public void finishStage(ResponseBuilder rb) {
//    if (!rb.doFacets || rb.stage != ResponseBuilder.STAGE_GET_FIELDS) return;
//    // wait until STAGE_GET_FIELDS
//    // so that "result" is already stored in the response (for aesthetics)
//
//    SimpleOrderedMap<List<NamedList<Object>>> tf = rb._pivots;
//
//    // get 'facet_counts' from the response
//    NamedList facetCounts = (NamedList) rb.rsp.getValues().get("facet_counts");
//    if (facetCounts == null) {
//      facetCounts = new NamedList();
//      rb.rsp.add("facet_counts", facetCounts);
//    }
//    facetCounts.add( PIVOT_KEY, tf );
//    rb._pivots = null;
//  }
//
//  public String getDescription() {
//    return "Handle Pivot (multi-level) Faceting";
//  }
//
//  public String getSource() {
//    return "$URL$";
//  }
}
