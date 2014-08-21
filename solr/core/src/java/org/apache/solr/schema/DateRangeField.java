package org.apache.solr.schema;

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

import com.spatial4j.core.shape.Shape;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.NumberRangePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.QParser;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @see NumberRangePrefixTreeStrategy
 * @see DateRangePrefixTree
 */
public class DateRangeField extends AbstractSpatialPrefixTreeFieldType<NumberRangePrefixTreeStrategy> {

  private static final String OP_PARAM = "op";//local-param to resolve SpatialOperation

  private final DateRangePrefixTree tree = DateRangePrefixTree.INSTANCE;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, addDegrees(args));
  }

  private Map<String, String> addDegrees(Map<String, String> args) {
    args.put("units", "degrees");//HACK!
    return args;
  }

  @Override
  protected NumberRangePrefixTreeStrategy newPrefixTreeStrategy(String fieldName) {
    return new NumberRangePrefixTreeStrategy(tree, fieldName);
  }

  @Override
  public List<StorableField> createFields(SchemaField field, Object val, float boost) {
    if (val instanceof Date || val instanceof Calendar)//From URP
      val = tree.toShape(val);
    return super.createFields(field, val, boost);
  }

  @Override
  protected Shape parseShape(String str) {
    try {
      return tree.parseShape(str);
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Couldn't parse date because: "+ e.getMessage(), e);
    }
  }

  @Override
  protected String shapeToString(Shape shape) {
    return shape.toString();//generally round-trips for DateRangePrefixTree
  }

  @Override
  protected SpatialArgs parseSpatialArgs(QParser parser, String externalVal) {
    //We avoid SpatialArgsParser entirely because it isn't very Solr-friendly
    final Shape shape = parseShape(externalVal);
    final SolrParams localParams = parser.getLocalParams();
    SpatialOperation op = SpatialOperation.Intersects;
    if (localParams != null) {
      String opStr = localParams.get(OP_PARAM);
      if (opStr != null)
        op = SpatialOperation.get(opStr);
    }
    return new SpatialArgs(op, shape);
  }

  @Override
  public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive, boolean maxInclusive) {
    if (!minInclusive || !maxInclusive)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "exclusive range boundary not supported");
    if (part1 == null)
      part1 = "*";
    if (part2 == null)
      part2 = "*";
    Shape shape = tree.toRangeShape(parseShape(part1), parseShape(part2));
    SpatialArgs spatialArgs = new SpatialArgs(SpatialOperation.Intersects, shape);
    return getQueryFromSpatialArgs(parser, field, spatialArgs);
  }
}
