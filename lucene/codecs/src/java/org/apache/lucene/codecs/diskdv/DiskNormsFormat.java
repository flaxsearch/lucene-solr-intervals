package org.apache.lucene.codecs.diskdv;

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

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.lucene45.Lucene45DocValuesConsumer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/** Norms format that keeps all norms on disk */
public final class DiskNormsFormat extends NormsFormat {

  @Override
  public DocValuesConsumer normsConsumer(SegmentWriteState state) throws IOException {
    return new Lucene45DocValuesConsumer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }

  @Override
  public DocValuesProducer normsProducer(SegmentReadState state) throws IOException {
    return new DiskDocValuesProducer(state, DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
  }
  
  static final String DATA_CODEC = "DiskNormsData";
  static final String DATA_EXTENSION = "dnvd";
  static final String META_CODEC = "DiskNormsMetadata";
  static final String META_EXTENSION = "dnvm";
}
