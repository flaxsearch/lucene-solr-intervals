package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.codecs.asserting.AssertingPostingsFormat;
import org.apache.lucene.codecs.bloom.TestBloomFilteredLucene41Postings;
import org.apache.lucene.codecs.diskdv.DiskDocValuesFormat;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.lucene41ords.Lucene41WithOrds;
import org.apache.lucene.codecs.lucene41vargap.Lucene41VarGapDocFreqInterval;
import org.apache.lucene.codecs.lucene41vargap.Lucene41VarGapFixedInterval;
import org.apache.lucene.codecs.lucene45.Lucene45DocValuesFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.FSTOrdPostingsFormat;
import org.apache.lucene.codecs.memory.FSTOrdPulsing41PostingsFormat;
import org.apache.lucene.codecs.memory.FSTPostingsFormat;
import org.apache.lucene.codecs.memory.FSTPulsing41PostingsFormat;
import org.apache.lucene.codecs.memory.MemoryDocValuesFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.codecs.nestedpulsing.NestedPulsingPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextDocValuesFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextPostingsFormat;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

/**
 * Codec that assigns per-field random postings formats.
 * <p>
 * The same field/format assignment will happen regardless of order,
 * a hash is computed up front that determines the mapping.
 * This means fields can be put into things like HashSets and added to
 * documents in different orders and the test will still be deterministic
 * and reproducable.
 */
public class RandomCodec extends Lucene46Codec {
  /** Shuffled list of postings formats to use for new mappings */
  private List<PostingsFormat> formats = new ArrayList<>();
  
  /** Shuffled list of docvalues formats to use for new mappings */
  private List<DocValuesFormat> dvFormats = new ArrayList<>();
  
  /** unique set of format names this codec knows about */
  public Set<String> formatNames = new HashSet<>();
  
  /** unique set of docvalues format names this codec knows about */
  public Set<String> dvFormatNames = new HashSet<>();

  /** memorized field->postingsformat mappings */
  // note: we have to sync this map even though its just for debugging/toString, 
  // otherwise DWPT's .toString() calls that iterate over the map can 
  // cause concurrentmodificationexception if indexwriter's infostream is on
  private Map<String,PostingsFormat> previousMappings = Collections.synchronizedMap(new HashMap<String,PostingsFormat>());
  private Map<String,DocValuesFormat> previousDVMappings = Collections.synchronizedMap(new HashMap<String,DocValuesFormat>());
  private final int perFieldSeed;

  @Override
  public PostingsFormat getPostingsFormatForField(String name) {
    PostingsFormat codec = previousMappings.get(name);
    if (codec == null) {
      codec = formats.get(Math.abs(perFieldSeed ^ name.hashCode()) % formats.size());
      if (codec instanceof SimpleTextPostingsFormat && perFieldSeed % 5 != 0) {
        // make simpletext rarer, choose again
        codec = formats.get(Math.abs(perFieldSeed ^ name.toUpperCase(Locale.ROOT).hashCode()) % formats.size());
      }
      previousMappings.put(name, codec);
      // Safety:
      assert previousMappings.size() < 10000: "test went insane";
    }
    return codec;
  }

  @Override
  public DocValuesFormat getDocValuesFormatForField(String name) {
    DocValuesFormat codec = previousDVMappings.get(name);
    if (codec == null) {
      codec = dvFormats.get(Math.abs(perFieldSeed ^ name.hashCode()) % dvFormats.size());
      if (codec instanceof SimpleTextDocValuesFormat && perFieldSeed % 5 != 0) {
        // make simpletext rarer, choose again
        codec = dvFormats.get(Math.abs(perFieldSeed ^ name.toUpperCase(Locale.ROOT).hashCode()) % dvFormats.size());
      }
      previousDVMappings.put(name, codec);
      // Safety:
      assert previousDVMappings.size() < 10000: "test went insane";
    }
    return codec;
  }

  public RandomCodec(Random random, Set<String> avoidCodecs) {
    this.perFieldSeed = random.nextInt();
    // TODO: make it possible to specify min/max iterms per
    // block via CL:
    int minItemsPerBlock = TestUtil.nextInt(random, 2, 100);
    int maxItemsPerBlock = 2*(Math.max(2, minItemsPerBlock-1)) + random.nextInt(100);
    int lowFreqCutoff = TestUtil.nextInt(random, 2, 100);

    add(avoidCodecs,
        new Lucene41PostingsFormat(minItemsPerBlock, maxItemsPerBlock),
        new FSTPostingsFormat(),
        new FSTOrdPostingsFormat(),
        new FSTPulsing41PostingsFormat(1 + random.nextInt(20)),
        new FSTOrdPulsing41PostingsFormat(1 + random.nextInt(20)),
        new DirectPostingsFormat(LuceneTestCase.rarely(random) ? 1 : (LuceneTestCase.rarely(random) ? Integer.MAX_VALUE : maxItemsPerBlock),
                                 LuceneTestCase.rarely(random) ? 1 : (LuceneTestCase.rarely(random) ? Integer.MAX_VALUE : lowFreqCutoff)),
        new Pulsing41PostingsFormat(1 + random.nextInt(20), minItemsPerBlock, maxItemsPerBlock),
        // add pulsing again with (usually) different parameters
        new Pulsing41PostingsFormat(1 + random.nextInt(20), minItemsPerBlock, maxItemsPerBlock),
        //TODO as a PostingsFormat which wraps others, we should allow TestBloomFilteredLucene41Postings to be constructed 
        //with a choice of concrete PostingsFormats. Maybe useful to have a generic means of marking and dealing 
        //with such "wrapper" classes?
        new TestBloomFilteredLucene41Postings(),                
        new MockRandomPostingsFormat(random),
        new NestedPulsingPostingsFormat(),
        new Lucene41WithOrds(TestUtil.nextInt(random, 1, 1000)),
        new Lucene41VarGapFixedInterval(TestUtil.nextInt(random, 1, 1000)),
        new Lucene41VarGapDocFreqInterval(TestUtil.nextInt(random, 1, 100), TestUtil.nextInt(random, 1, 1000)),
        new SimpleTextPostingsFormat(),
        new AssertingPostingsFormat(),
        new MemoryPostingsFormat(true, random.nextFloat()),
        new MemoryPostingsFormat(false, random.nextFloat()));
    
    addDocValues(avoidCodecs,
        new Lucene45DocValuesFormat(),
        new DiskDocValuesFormat(),
        new MemoryDocValuesFormat(),
        new SimpleTextDocValuesFormat(),
        new AssertingDocValuesFormat());

    Collections.shuffle(formats, random);
    Collections.shuffle(dvFormats, random);

    // Avoid too many open files:
    if (formats.size() > 4) {
      formats = formats.subList(0, 4);
    }
    if (dvFormats.size() > 4) {
      dvFormats = dvFormats.subList(0, 4);
    }
  }

  public RandomCodec(Random random) {
    this(random, Collections.<String> emptySet());
  }

  private final void add(Set<String> avoidCodecs, PostingsFormat... postings) {
    for (PostingsFormat p : postings) {
      if (!avoidCodecs.contains(p.getName())) {
        formats.add(p);
        formatNames.add(p.getName());
      }
    }
  }
  
  private final void addDocValues(Set<String> avoidCodecs, DocValuesFormat... docvalues) {
    for (DocValuesFormat d : docvalues) {
      if (!avoidCodecs.contains(d.getName())) {
        dvFormats.add(d);
        dvFormatNames.add(d.getName());
      }
    }
  }

  @Override
  public String toString() {
    return super.toString() + ": " + previousMappings.toString() + ", docValues:" + previousDVMappings.toString();
  }
}
