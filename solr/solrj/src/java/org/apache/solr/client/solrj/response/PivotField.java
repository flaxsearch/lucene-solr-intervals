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

package org.apache.solr.client.solrj.response;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PivotField implements Serializable
{
  final String  _field;
  final Object  _value;
  final int     _count;
  final List<PivotField> _pivot;
  final Map<String,FieldStatsInfo> _statsInfo;

  /**
   * @deprecated Use {@link #PivotField(String,Object,int,List,Map)} with a null <code>statsInfo</code>
   */
  @Deprecated
  public PivotField( String f, Object v, int count, List<PivotField> pivot) {
    this(f, v, count, pivot, null);
  }

  public PivotField( String f, Object v, int count, List<PivotField> pivot, Map<String,FieldStatsInfo> statsInfo)
  {
    _field = f;
    _value = v;
    _count = count;
    _pivot = pivot;
    _statsInfo = statsInfo;
  }
   
  public String getField() {
   return _field;
  }

  public Object getValue() {
    return _value;
  }

  public int getCount() {
    return _count;
  }

  public List<PivotField> getPivot() {
    return _pivot;
  }
   
  public Map<String,FieldStatsInfo> getFieldStatsInfo() {
    return _statsInfo;
  }

  @Override
  public String toString()
  {
    return _field + ":" + _value + " ["+_count+"] "+_pivot;
  }

  public void write( PrintStream out, int indent )
  {
    for( int i=0; i<indent; i++ ) {
      out.print( "  " );
    }
    out.print( _field + "=" + _value + " ("+_count+")" );
    if (null != _statsInfo) {
      out.print( "->stats:[" ); 
      for( FieldStatsInfo fieldStatsInfo : _statsInfo.values() ) {
        out.print(fieldStatsInfo.toString());
        out.print(",");
      }
      out.print("]");
    }
    out.println();
    if( _pivot != null ) {
      for( PivotField p : _pivot ) {
        p.write( out, indent+1 );
      }
    }
  }
}
