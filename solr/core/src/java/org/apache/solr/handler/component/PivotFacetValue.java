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

import java.util.BitSet;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.util.PivotListEntry;

/**
 * Models a single (value, count) pair that will exist in the collection of values for a 
 * {@link PivotFacetField} parent.  This <code>PivotFacetValue</code> may itself have a 
 * nested {@link PivotFacetField} child
 *
 * @see PivotFacetField
 * @see PivotFacetFieldValueCollection
 */
@SuppressWarnings("rawtypes")
public class PivotFacetValue {
    
  private final BitSet sourceShards = new BitSet();
  private final PivotFacetField parentPivot;
  private final Comparable value;
  // child can't be final, circular ref on construction
  private PivotFacetField childPivot = null; 
  private int count; // mutable
  
  private PivotFacetValue(PivotFacetField parent, Comparable val) { 
    this.parentPivot = parent;
    this.value = val;
  }

  /** 
   * The value of the asssocated field modeled by this <code>PivotFacetValue</code>. 
   * May be null if this <code>PivotFacetValue</code> models the count for docs 
   * "missing" the field value.
   *
   * @see FacetParams#FACET_MISSING
   */
  public Comparable getValue() { return value; }

  /** The count corrisponding to the value modeled by this <code>PivotFacetValue</code> */
  public int getCount() { return count; }

  /** 
   * The {@link PivotFacetField} corrisponding to the nested child pivot for this 
   * <code>PivotFacetValue</code>. May be null if this object is the leaf of a pivot.
   */
  public PivotFacetField getChildPivot() { return childPivot; }


  /** 
   * A recursive method that walks up the tree of pivot fields/values to build 
   * a list of the String representations of the values that lead down to this 
   * PivotFacetValue.
   *
   * @return a mutable List of the pivot value Strings leading down to and including 
   *      this pivot value, will never be null but may contain nulls
   * @see PivotFacetField#getValuePath
   */
  public List<String> getValuePath() {
    List<String> out = parentPivot.getValuePath();

    // Note: this code doesn't play nice with custom FieldTypes -- see SOLR-6330

    if (null == value) {
      out.add(null);
    } else if (value instanceof Date) {
      out.add(TrieDateField.formatExternal((Date) value));
    } else {
      out.add(value.toString());
    }
    return out;
  }

  /**
   * A recursive method to construct a new <code>PivotFacetValue</code> object from 
   * the contents of the {@link NamedList} provided by the specified shard, relative 
   * to the specified field.  
   *
   * If the <code>NamedList</code> contains data for a child {@link PivotFacetField} 
   * that will be recursively built as well.
   *
   * @see PivotFacetField#createFromListOfNamedLists
   * @param shardNumber the id of the shard that provided this data
   * @param rb The response builder of the current request
   * @param parentField the parent field in the current pivot associated with this value
   * @param pivotData the data from the specified shard for this pivot value
   */
  @SuppressWarnings("unchecked")
  public static PivotFacetValue createFromNamedList(int shardNumber, ResponseBuilder rb, PivotFacetField parentField, NamedList<Object> pivotData) {
    
    Comparable pivotVal = null;
    int pivotCount = 0;
    List<NamedList<Object>> childPivotData = null;
    
    for (int i = 0; i < pivotData.size(); i++) {
      String key = pivotData.getName(i);
      Object value = pivotData.getVal(i);
      PivotListEntry entry = PivotListEntry.get(key);
      
      switch (entry) {

      case VALUE: 
        pivotVal = (Comparable)value;
        break;
      case FIELD:
        assert parentField.field.equals(value) 
          : "Parent Field mismatch: " + parentField.field + "!=" + value;
        break;
      case COUNT:
        pivotCount = (Integer)value;
        break;
      case PIVOT:
        childPivotData = (List<NamedList<Object>>)value;
        break;
      default:
        throw new RuntimeException("PivotListEntry contains unaccounted for item: " + entry);
      }
    }    

    PivotFacetValue newPivotFacet = new PivotFacetValue(parentField, pivotVal);
    newPivotFacet.count = pivotCount;
    newPivotFacet.sourceShards.set(shardNumber);
    
    newPivotFacet.childPivot = PivotFacetField.createFromListOfNamedLists(shardNumber, rb, newPivotFacet, childPivotData);
    
    return newPivotFacet;
  }

  /** 
   * A <b>NON-Recursive</b> method indicating if the specified shard has already
   * contributed to the count for this value.
   */
  public boolean shardHasContributed(int shardNum) {
    return sourceShards.get(shardNum);
  }
  
  /** 
   * A recursive method for generating a NamedList from this value suitable for 
   * including in a pivot facet response to the original distributed request.
   *
   * @see PivotFacetField#convertToListOfNamedLists
   */
  public NamedList<Object> convertToNamedList() {
    NamedList<Object> newList = new SimpleOrderedMap<>();
    newList.add(PivotListEntry.FIELD.getName(), parentPivot.field);
    newList.add(PivotListEntry.VALUE.getName(), value);    
    newList.add(PivotListEntry.COUNT.getName(), count);      
    if (childPivot != null && childPivot.convertToListOfNamedLists() != null) {
      newList.add(PivotListEntry.PIVOT.getName(), childPivot.convertToListOfNamedLists());
    }
    return newList;
  }      
  
  /**
   * Merges in the count contributions from the specified shard for each.
   * This method is recursive if the shard data includes sub-pivots
   *
   * @see PivotFacetField#contributeFromShard
   * @see PivotFacetField#createFromListOfNamedLists
   */
  public void mergeContributionFromShard(int shardNumber, ResponseBuilder rb, NamedList<Object> value) {
    assert null != value : "can't merge in null data";
    
    if (!shardHasContributed(shardNumber)) {
      sourceShards.set(shardNumber);      
      count += PivotFacetHelper.getCount(value);
    }
    
    List<NamedList<Object>> shardChildPivots = PivotFacetHelper.getPivots(value);
    // sub pivot -- we may not have seen this yet depending on refinement
    if (null == childPivot) {
      childPivot = PivotFacetField.createFromListOfNamedLists(shardNumber, rb,  this,  shardChildPivots);
    } else {
      childPivot.contributeFromShard(shardNumber, rb, shardChildPivots);
    }
  }
  
  public String toString(){
    return String.format(Locale.ROOT, "F:%s V:%s Co:%d Ch?:%s", 
                         parentPivot.field, value, count, (this.childPivot !=null));
  }
  
}
