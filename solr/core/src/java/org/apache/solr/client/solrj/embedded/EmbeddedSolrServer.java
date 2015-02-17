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

package org.apache.solr.client.solrj.embedded;

import com.google.common.base.Strings;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.SolrRequestParsers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * SolrClient that connects directly to SolrCore.
 * <p>
 * TODO -- this implementation sends the response to XML and then parses it.  
 * It *should* be able to convert the response directly into a named list.
 * 
 *
 * @since solr 1.3
 */
public class EmbeddedSolrServer extends SolrClient
{
  protected final CoreContainer coreContainer;
  protected final String coreName;
  private final SolrRequestParsers _parser;
  
  /**
   * Use the other constructor using a CoreContainer and a name.
   */
  public EmbeddedSolrServer(SolrCore core)
  {
    this(core.getCoreDescriptor().getCoreContainer(), core.getName());
  }
    
  /**
   * Creates a SolrServer.
   * @param coreContainer the core container
   * @param coreName the core name
   */
  public EmbeddedSolrServer(CoreContainer coreContainer, String coreName)
  {
    if ( coreContainer == null ) {
      throw new NullPointerException("CoreContainer instance required");
    }
    if (Strings.isNullOrEmpty(coreName))
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core name cannot be empty");
    this.coreContainer = coreContainer;
    this.coreName = coreName;
    _parser = new SolrRequestParsers( null );
  }
  
  @Override
  public NamedList<Object> request(SolrRequest request) throws SolrServerException, IOException 
  {
    String path = request.getPath();
    if( path == null || !path.startsWith( "/" ) ) {
      path = "/select";
    }

    // Check for cores action
    SolrCore core =  coreContainer.getCore( coreName );
    if( core == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
                               "No such core: " + coreName );
    }
    
    SolrParams params = request.getParams();
    if( params == null ) {
      params = new ModifiableSolrParams();
    }
    
    // Extract the handler from the path or params
    SolrRequestHandler handler = core.getRequestHandler( path );
    if( handler == null ) {
      if( "/select".equals( path ) || "/select/".equalsIgnoreCase( path) ) {
        String qt = params.get( CommonParams.QT );
        handler = core.getRequestHandler( qt );
        if( handler == null ) {
          throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+qt);
        }
      }
      // Perhaps the path is to manage the cores
      if (handler == null) {
        handler = coreContainer.getRequestHandler(path);
      }
    }
    if( handler == null ) {
      core.close();
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST, "unknown handler: "+path );
    }

    SolrQueryRequest req = null;
    try {
      req = _parser.buildRequestFrom( core, params, request.getContentStreams() );
      req.getContext().put( "path", path );
      SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      
      core.execute( handler, req, rsp );
      if( rsp.getException() != null ) {
        if(rsp.getException() instanceof SolrException) {
          throw rsp.getException();
        }
        throw new SolrServerException( rsp.getException() );
      }
      
      // Check if this should stream results
      if( request.getStreamingResponseCallback() != null ) {
        try {
          final StreamingResponseCallback callback = request.getStreamingResponseCallback();
          BinaryResponseWriter.Resolver resolver = 
            new BinaryResponseWriter.Resolver( req, rsp.getReturnFields()) 
          {
            @Override
            public void writeResults(ResultContext ctx, JavaBinCodec codec) throws IOException {
              // write an empty list...
              SolrDocumentList docs = new SolrDocumentList();
              docs.setNumFound( ctx.docs.matches() );
              docs.setStart( ctx.docs.offset() );
              docs.setMaxScore( ctx.docs.maxScore() );
              codec.writeSolrDocumentList( docs );
              
              // This will transform
              writeResultsBody( ctx, codec );
            }
          };
          

          ByteArrayOutputStream out = new ByteArrayOutputStream();
          new JavaBinCodec(resolver) {

            @Override
            public void writeSolrDocument(SolrDocument doc) {
              callback.streamSolrDocument( doc );
              //super.writeSolrDocument( doc, fields );
            }

            @Override
            public void writeSolrDocumentList(SolrDocumentList docs) throws IOException {
              if( docs.size() > 0 ) {
                SolrDocumentList tmp = new SolrDocumentList();
                tmp.setMaxScore( docs.getMaxScore() );
                tmp.setNumFound( docs.getNumFound() );
                tmp.setStart( docs.getStart() );
                docs = tmp;
              }
              callback.streamDocListInfo( docs.getNumFound(), docs.getStart(), docs.getMaxScore() );
              super.writeSolrDocumentList(docs);
            }
            
          }.marshal(rsp.getValues(), out);

          InputStream in = new ByteArrayInputStream(out.toByteArray());
          return (NamedList<Object>) new JavaBinCodec(resolver).unmarshal(in);
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
      
      // Now write it out
      NamedList<Object> normalized = BinaryResponseWriter.getParsedResponse(req, rsp);
      return normalized;
    } catch( IOException | SolrException iox ) {
      throw iox;
    } catch( Exception ex ) {
      throw new SolrServerException( ex );
    }
    finally {
      if (req != null) req.close();
      core.close();
      SolrRequestInfo.clearRequestInfo();
    }
  }
  
  /**
   * Shutdown all cores within the EmbeddedSolrServer instance
   */
  @Override
  @Deprecated
  public void shutdown() {
    coreContainer.shutdown();
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }
  
  /**
   * Getter method for the CoreContainer
   * @return the core container
   */
  public CoreContainer getCoreContainer() {
    return coreContainer;
  }
}
