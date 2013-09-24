package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.old.OldFacetsAccumulator;
import org.apache.lucene.facet.old.ScoredDocIDs;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.partitions.PartitionsFacetResultsHandler;
import org.apache.lucene.facet.sampling.Sampler.SampleResult;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

/**
 * Wrap any Facets Accumulator with sampling.
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()}.
 * 
 * @lucene.experimental
 */
public class SamplingWrapper extends OldFacetsAccumulator {

  private OldFacetsAccumulator delegee;
  private Sampler sampler;

  public SamplingWrapper(OldFacetsAccumulator delegee, Sampler sampler) {
    super(delegee.searchParams, delegee.indexReader, delegee.taxonomyReader);
    this.delegee = delegee;
    this.sampler = sampler;
  }

  @Override
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {
    // Replacing the original searchParams with the over-sampled (and without statistics-compute)
    FacetSearchParams original = delegee.searchParams;
    boolean shouldOversample = sampler.samplingParams.shouldOverSample();
   
    if (shouldOversample) {
      delegee.searchParams = sampler.overSampledSearchParams(original);
    }
    
    SampleResult sampleSet = sampler.getSampleSet(docids);

    List<FacetResult> sampleRes = delegee.accumulate(sampleSet.docids);

    List<FacetResult> results = new ArrayList<FacetResult>();
    SampleFixer sampleFixer = sampler.samplingParams.getSampleFixer();
    
    for (FacetResult fres : sampleRes) {
      // for sure fres is not null because this is guaranteed by the delegee.
      FacetRequest fr = fres.getFacetRequest();
      PartitionsFacetResultsHandler frh = createFacetResultsHandler(fr, createOrdinalValueResolver(fr));
      if (sampleFixer != null) {
        // fix the result of current request
        sampleFixer.fixResult(docids, fres, sampleSet.actualSampleRatio); 
        fres = frh.rearrangeFacetResult(fres); // let delegee's handler do any
      }
      
      if (shouldOversample) {
        // Using the sampler to trim the extra (over-sampled) results
        fres = sampler.trimResult(fres);
      }
      
      // final labeling if allowed (because labeling is a costly operation)
      if (fres.getFacetResultNode().ordinal == TaxonomyReader.INVALID_ORDINAL) {
        // category does not exist, add an empty result
        results.add(emptyResult(fres.getFacetResultNode().ordinal, fr));
      } else {
        frh.labelResult(fres);
        results.add(fres);
      }
    }

    if (shouldOversample) {
      delegee.searchParams = original; // Back to original params
    }
    
    return results; 
  }

  @Override
  public double getComplementThreshold() {
    return delegee.getComplementThreshold();
  }

  @Override
  public void setComplementThreshold(double complementThreshold) {
    delegee.setComplementThreshold(complementThreshold);
  }

}
