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

package org.apache.solr.schema;

import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.QParser;
import org.apache.lucene.index.GeneralField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.StorableField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.solr.response.TextResponseWriter;

import java.util.Map;
import java.io.IOException;
/**
 * A legacy numeric field type that encodes "Float" values as simple Strings.
 * This class should not be used except by people with existing indexes that
 * contain numeric values indexed as Strings.  
 * New schemas should use {@link TrieFloatField}.
 *
 * <p>
 * Field values will sort numerically, but Range Queries (and other features 
 * that rely on numeric ranges) will not work as expected: values will be 
 * evaluated in unicode String order, not numeric order.
 * </p>
 * 
 * @see TrieFloatField
 */
public class FloatField extends PrimitiveFieldType implements FloatValueFieldType {

  private static final FieldCache.FloatParser PARSER = new FieldCache.FloatParser() {
    
    @Override
    public TermsEnum termsEnum(Terms terms) throws IOException {
      return terms.iterator(null);
    }

    @Override
    public float parseFloat(BytesRef term) {
      return Float.parseFloat(term.utf8ToString());
    }
  };

  @Override
  protected void init(IndexSchema schema, Map<String,String> args) {
    super.init(schema, args);
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
  }

  @Override
  public SortField getSortField(SchemaField field,boolean reverse) {
    field.checkSortability();
    return new SortField(field.name,SortField.Type.FLOAT, reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource(qparser);
    return new FloatFieldSource(field.name, PARSER);
  }

  @Override
  public void write(TextResponseWriter writer, String name, StorableField f) throws IOException {
    String s = f.stringValue();

    // these values may be from a legacy lucene index, which may
    // not be properly formatted in some output formats, or may
    // incorrectly have a zero length.

    if (s.length()==0) {
      // zero length value means someone mistakenly indexed the value
      // instead of simply leaving it out.  Write a null value instead of a numeric.
      writer.writeNull(name);
      return;
    }

    try {
      float fval = Float.parseFloat(s);
      writer.writeFloat(name, fval);
    } catch (NumberFormatException e){
      // can't parse - write out the contents as a string so nothing is lost and
      // clients don't get a parse error.
      writer.writeStr(name, s, true);
    }
  }

  @Override
  public Float toObject(StorableField f) {
    return Float.valueOf( toExternal(f) );
  }
}
