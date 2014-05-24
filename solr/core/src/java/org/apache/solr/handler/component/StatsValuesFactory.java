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

import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.*;

/**
 * Factory class for creating instance of {@link org.apache.solr.handler.component.StatsValues}
 */
public class StatsValuesFactory {

  /**
   * Creates an instance of StatsValues which supports values from a field of the given FieldType
   *
   * @param sf SchemaField for the field whose statistics will be created by the resulting StatsValues
   * @return Instance of StatsValues that will create statistics from values from a field of the given type
   */
  public static StatsValues createStatsValues(SchemaField sf, boolean calcDistinct) {
    // TODO: allow for custom field types
    FieldType fieldType = sf.getType();
    if (TrieDateField.class.isInstance(fieldType)) {
      return new DateStatsValues(sf, calcDistinct);
    } else if (TrieField.class.isInstance(fieldType)) {
      return new NumericStatsValues(sf, calcDistinct);
    } else if (StrField.class.isInstance(fieldType)) {
      return new StringStatsValues(sf, calcDistinct);
    } else if (sf.getType().getClass().equals(EnumField.class)) {
      return new EnumStatsValues(sf, calcDistinct);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Field type " + fieldType + " is not currently supported");
    }
  }
}

/**
 * Abstract implementation of {@link org.apache.solr.handler.component.StatsValues} that provides the default behavior
 * for most StatsValues implementations.
 *
 * There are very few requirements placed on what statistics concrete implementations should collect, with the only required
 * statistics being the minimum and maximum values.
 */
abstract class AbstractStatsValues<T> implements StatsValues {
  private static final String FACETS = "facets";
  final protected SchemaField sf;
  final protected FieldType ft;
  protected T max;
  protected T min;
  protected long missing;
  protected long count;
  protected long countDistinct;
  protected Set<T> distinctValues;
  private ValueSource valueSource;
  protected FunctionValues values;
  protected boolean calcDistinct = false;
  
  // facetField   facetValue
  protected Map<String, Map<String, StatsValues>> facets = new HashMap<>();

  protected AbstractStatsValues(SchemaField sf, boolean calcDistinct) {
    this.sf = sf;
    this.ft = sf.getType();
    this.distinctValues = new TreeSet<>();
    this.calcDistinct = calcDistinct;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(NamedList stv) {
    count += (Long) stv.get("count");
    missing += (Long) stv.get("missing");
    if (calcDistinct) {
      distinctValues.addAll((Collection<T>) stv.get("distinctValues"));
      countDistinct = distinctValues.size();
    }

    updateMinMax((T) stv.get("min"), (T) stv.get("max"));
    updateTypeSpecificStats(stv);

    NamedList f = (NamedList) stv.get(FACETS);
    if (f == null) {
      return;
    }

    for (int i = 0; i < f.size(); i++) {
      String field = f.getName(i);
      NamedList vals = (NamedList) f.getVal(i);
      Map<String, StatsValues> addTo = facets.get(field);
      if (addTo == null) {
        addTo = new HashMap<>();
        facets.put(field, addTo);
      }
      for (int j = 0; j < vals.size(); j++) {
        String val = vals.getName(j);
        StatsValues vvals = addTo.get(val);
        if (vvals == null) {
          vvals = StatsValuesFactory.createStatsValues(sf, calcDistinct);
          addTo.put(val, vvals);
        }
        vvals.accumulate((NamedList) vals.getVal(j));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(BytesRef value, int count) {
    T typedValue = (T)ft.toObject(sf, value);
    accumulate(typedValue, count);
  }

  public void accumulate(T value, int count) {
    this.count += count;
    if (calcDistinct) {
      distinctValues.add(value);
      countDistinct = distinctValues.size();
    }
    updateMinMax(value, value);
    updateTypeSpecificStats(value, count);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void missing() {
    missing++;
  }
   
  /**
   * {@inheritDoc}
   */
  @Override
  public void addMissing(int count) {
    missing += count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addFacet(String facetName, Map<String, StatsValues> facetValues) {
    facets.put(facetName, facetValues);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NamedList<?> getStatsValues() {
    NamedList<Object> res = new SimpleOrderedMap<>();

    res.add("min", min);
    res.add("max", max);
    res.add("count", count);
    res.add("missing", missing);
    if (calcDistinct) {
      res.add("distinctValues", distinctValues);
      res.add("countDistinct", countDistinct);
    }

    addTypeSpecificStats(res);

     // add the facet stats
    NamedList<NamedList<?>> nl = new SimpleOrderedMap<>();
    for (Map.Entry<String, Map<String, StatsValues>> entry : facets.entrySet()) {
      NamedList<NamedList<?>> nl2 = new SimpleOrderedMap<>();
      nl.add(entry.getKey(), nl2);
      for (Map.Entry<String, StatsValues> e2 : entry.getValue().entrySet()) {
        nl2.add(e2.getKey(), e2.getValue().getStatsValues());
      }
    }
    res.add(FACETS, nl);
    return res;
  }

  public void setNextReader(AtomicReaderContext ctx) throws IOException {
    if (valueSource == null) {
      valueSource = ft.getValueSource(sf, null);
    }
    values = valueSource.getValues(Collections.emptyMap(), ctx);
  }

  /**
   * Updates the minimum and maximum statistics based on the given values
   *
   * @param min Value that the current minimum should be updated against
   * @param max Value that the current maximum should be updated against
   */
  protected abstract void updateMinMax(T min, T max);

  /**
   * Updates the type specific statistics based on the given value
   *
   * @param value Value the statistics should be updated against
   * @param count Number of times the value is being accumulated
   */
  protected abstract void updateTypeSpecificStats(T value, int count);

  /**
   * Updates the type specific statistics based on the values in the given list
   *
   * @param stv List containing values the current statistics should be updated against
   */
  protected abstract void updateTypeSpecificStats(NamedList stv);

  /**
   * Add any type specific statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  protected abstract void addTypeSpecificStats(NamedList<Object> res);
}

 /**
 * Implementation of StatsValues that supports Double values
 */
class NumericStatsValues extends AbstractStatsValues<Number> {

  double sum;
  double sumOfSquares;

  public NumericStatsValues(SchemaField sf, boolean calcDistinct) {
    super(sf, calcDistinct);
    min = Double.POSITIVE_INFINITY;
    max = Double.NEGATIVE_INFINITY;
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      accumulate((Number) values.objectVal(docID), 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(NamedList stv) {
    sum += ((Number)stv.get("sum")).doubleValue();
    sumOfSquares += ((Number)stv.get("sumOfSquares")).doubleValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Number v, int count) {
    double value = v.doubleValue();
    sumOfSquares += (value * value * count); // for std deviation
    sum += value * count;
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(Number min, Number max) {
    this.min = Math.min(this.min.doubleValue(), min.doubleValue());
    this.max = Math.max(this.max.doubleValue(), max.doubleValue());
  }

  /**
   * Adds sum, sumOfSquares, mean and standard deviation statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    res.add("sum", sum);
    res.add("sumOfSquares", sumOfSquares);
    res.add("mean", sum / count);
    res.add("stddev", getStandardDeviation());
  }

  /**
   * Calculates the standard deviation statistic
   *
   * @return Standard deviation statistic
   */
  private double getStandardDeviation() {
    if (count <= 1.0D) {
      return 0.0D;
    }

    return Math.sqrt(((count * sumOfSquares) - (sum * sum)) / (count * (count - 1.0D)));
  }
}

/**
 * Implementation of StatsValues that supports EnumField values
 */
class EnumStatsValues extends AbstractStatsValues<EnumFieldValue> {

  public EnumStatsValues(SchemaField sf, boolean calcDistinct) {
    super(sf, calcDistinct);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      Integer intValue = (Integer) values.objectVal(docID);
      String stringValue = values.strVal(docID);
      EnumFieldValue enumFieldValue = new EnumFieldValue(intValue, stringValue);
      accumulate(enumFieldValue, 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  protected void updateMinMax(EnumFieldValue min, EnumFieldValue max) {
    if (max != null) {
      if (max.compareTo(this.max) > 0)
        this.max = max;
    }
    if (this.min == null)
      this.min = min;
    else if (this.min.compareTo(min) > 0)
      this.min = min;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(EnumFieldValue value, int count) {
    // No type specific stats
  }

  /**
   * Adds no type specific statistics
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    // Add no statistics
  }


}

/**
 * /**
 * Implementation of StatsValues that supports Date values
 */
class DateStatsValues extends AbstractStatsValues<Date> {

  private long sum = 0;
  double sumOfSquares = 0;

  public DateStatsValues(SchemaField sf, boolean calcDistinct) {
    super(sf, calcDistinct);
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      accumulate((Date) values.objectVal(docID), 1);
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    Date date = (Date) stv.get("sum");
    if (date != null) {
      sum += date.getTime();
      sumOfSquares += ((Number)stv.get("sumOfSquares")).doubleValue();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void updateTypeSpecificStats(Date v, int count) {
    long value = v.getTime();
    sumOfSquares += (value * value * count); // for std deviation
    sum += value * count;
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(Date min, Date max) {
    if(null != min && (this.min==null || this.min.after(min))) {
      this.min = min;
    }
    if(null != max && (this.max==null || this.max.before(max))) {
      this.max = max;
    }
  }

  /**
   * Adds sum and mean statistics to the given NamedList
   *
   * @param res NamedList to add the type specific statistics too
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    if(sum<=0) {
      return; // date==0 is meaningless
    }
    res.add("sum", new Date(sum));
    if (count > 0) {
      res.add("mean", new Date(sum / count));
    }
    res.add("sumOfSquares", sumOfSquares);
    res.add("stddev", getStandardDeviation());
  }
  

  
  /**
   * Calculates the standard deviation.  For dates, this is really the MS deviation
   *
   * @return Standard deviation statistic
   */
  private double getStandardDeviation() {
    if (count <= 1) {
      return 0.0D;
    }
    return Math.sqrt(((count * sumOfSquares) - (sum * sum)) / (count * (count - 1.0D)));
  }
}

/**
 * Implementation of StatsValues that supports String values
 */
class StringStatsValues extends AbstractStatsValues<String> {

  public StringStatsValues(SchemaField sf, boolean calcDistinct) {
    super(sf, calcDistinct);
  }

  @Override
  public void accumulate(int docID) {
    if (values.exists(docID)) {
      String value = values.strVal(docID);
      if (value != null)
        accumulate(value, 1);
      else
        missing();
    } else {
      missing();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(NamedList stv) {
    // No type specific stats
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void updateTypeSpecificStats(String value, int count) {
    // No type specific stats
  }

   /**
   * {@inheritDoc}
   */
  @Override
  protected void updateMinMax(String min, String max) {
    this.max = max(this.max, max);
    this.min = min(this.min, min);
  }

  /**
   * Adds no type specific statistics
   */
  @Override
  protected void addTypeSpecificStats(NamedList<Object> res) {
    // Add no statistics
  }

  /**
   * Determines which of the given Strings is the maximum, as computed by {@link String#compareTo(String)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered greater by {@link String#compareTo(String)}, str2 otherwise
   */
  private static String max(String str1, String str2) {
    if (str1 == null) {
      return str2;
    } else if (str2 == null) {
      return str1;
    }
    return (str1.compareTo(str2) > 0) ? str1 : str2;
  }

  /**
   * Determines which of the given Strings is the minimum, as computed by {@link String#compareTo(String)}
   *
   * @param str1 String to compare against b
   * @param str2 String compared against a
   * @return str1 if it is considered less by {@link String#compareTo(String)}, str2 otherwise
   */
  private static String min(String str1, String str2) {
    if (str1 == null) {
      return str2;
    } else if (str2 == null) {
      return str1;
    }
    return (str1.compareTo(str2) < 0) ? str1 : str2;
  }
}
