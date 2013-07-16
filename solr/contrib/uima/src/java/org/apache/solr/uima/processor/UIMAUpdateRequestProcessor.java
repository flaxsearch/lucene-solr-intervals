package org.apache.solr.uima.processor;

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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.uima.processor.SolrUIMAConfiguration.MapField;
import org.apache.lucene.analysis.uima.ae.AEProvider;
import org.apache.lucene.analysis.uima.ae.AEProviderFactory;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Update document(s) to be indexed with UIMA extracted information
 *
 */
public class UIMAUpdateRequestProcessor extends UpdateRequestProcessor {

  private final Logger log = LoggerFactory.getLogger(UIMAUpdateRequestProcessor.class);

  SolrUIMAConfiguration solrUIMAConfiguration;

  private AEProvider aeProvider;
  
  private SolrCore solrCore;

  public UIMAUpdateRequestProcessor(UpdateRequestProcessor next, SolrCore solrCore,
      SolrUIMAConfiguration config) {
    super(next);
    initialize(solrCore, config);
  }

  private void initialize(SolrCore solrCore, SolrUIMAConfiguration config) {
    this.solrCore = solrCore;
    solrUIMAConfiguration = config;
    aeProvider = AEProviderFactory.getInstance().getAEProvider(solrCore.getName(),
            solrUIMAConfiguration.getAePath(), solrUIMAConfiguration.getRuntimeParameters());
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    String text = null;
    try {
      /* get Solr document */
      SolrInputDocument solrInputDocument = cmd.getSolrInputDocument();

      /* get the fields to analyze */
      String[] texts = getTextsToAnalyze(solrInputDocument);
      for (String currentText : texts) {
        text = currentText;
        if (text != null && text.length() > 0) {
          /* process the text value */
          JCas jcas = processText(text);

          UIMAToSolrMapper uimaToSolrMapper = new UIMAToSolrMapper(solrInputDocument, jcas);
          /* get field mapping from config */
          Map<String, Map<String, MapField>> typesAndFeaturesFieldsMap = solrUIMAConfiguration
              .getTypesFeaturesFieldsMapping();
          /* map type features on fields */
          for (String typeFQN : typesAndFeaturesFieldsMap.keySet()) {
            uimaToSolrMapper.map(typeFQN, typesAndFeaturesFieldsMap.get(typeFQN));
          }
        }
      }
    } catch (Exception e) {
      String logField = solrUIMAConfiguration.getLogField();
      if(logField == null){
        SchemaField uniqueKeyField = cmd.getReq().getSchema().getUniqueKeyField();
        if(uniqueKeyField != null){
          logField = uniqueKeyField.getName();
        }
      }
      String optionalFieldInfo = logField == null ? "." :
        new StringBuilder(". ").append(logField).append("=")
        .append((String)cmd.getSolrInputDocument().getField(logField).getValue())
        .append(", ").toString();
      int len;
      String debugString;
      if (text != null && text.length() > 0) {
        len = Math.min(text.length(), 100);
        debugString = new StringBuilder(" text=\"").append(text.substring(0, len)).append("...\"").toString();
      }
      else {
        debugString = " null text";
      }
      if (solrUIMAConfiguration.isIgnoreErrors()) {
        log.warn("skip the text processing due to {}",new StringBuilder()
          .append(e.getLocalizedMessage()).append(optionalFieldInfo)
          .append(debugString));
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            new StringBuilder("processing error ")
              .append(e.getLocalizedMessage()).append(optionalFieldInfo)
              .append(debugString).toString(), e);
      }
    }
    super.processAdd(cmd);
  }

  /*
   * get the texts to analyze from the corresponding fields
   */
  private String[] getTextsToAnalyze(SolrInputDocument solrInputDocument) {
    String[] fieldsToAnalyze = solrUIMAConfiguration.getFieldsToAnalyze();
    boolean merge = solrUIMAConfiguration.isFieldsMerging();
    String[] textVals;
    if (merge) {
      StringBuilder unifiedText = new StringBuilder("");
      for (String aFieldsToAnalyze : fieldsToAnalyze) {
        unifiedText.append(String.valueOf(solrInputDocument.getFieldValue(aFieldsToAnalyze)));
      }
      textVals = new String[1];
      textVals[0] = unifiedText.toString();
    } else {
      textVals = new String[fieldsToAnalyze.length];
      for (int i = 0; i < fieldsToAnalyze.length; i++) {
        textVals[i] = String.valueOf(solrInputDocument.getFieldValue(fieldsToAnalyze[i]));
      }
    }
    return textVals;
  }

  /* process a field value executing UIMA the CAS containing it as document text */
  private JCas processText(String textFieldValue) throws ResourceInitializationException,
          AnalysisEngineProcessException {
    if (log.isDebugEnabled()) {
      log.debug("Analyzing text");
    }
    /* get the UIMA analysis engine */
    AnalysisEngine ae = aeProvider.getAE();

    /* create a JCas which contain the text to analyze */
    JCas jcas = ae.newJCas();
    jcas.setDocumentText(textFieldValue);

    /* perform analysis on text field */
    ae.process(jcas);
    if (log.isDebugEnabled()) {
      log.debug("Text processing completed");
    }
    return jcas;
  }

}
