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

import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Filter;
import java.util.Map;
import java.util.TreeMap;

public class JettyConfig {

  final int port;

  public final String context;

  final boolean stopAtShutdown;

  final Map<ServletHolder, String> extraServlets;

  final Map<Class<? extends Filter>, String> extraFilters;

  final SSLConfig sslConfig;

  private JettyConfig(int port, String context, boolean stopAtShutdown, Map<ServletHolder, String> extraServlets,
                      Map<Class<? extends Filter>, String> extraFilters, SSLConfig sslConfig) {
    this.port = port;
    this.context = context;
    this.stopAtShutdown = stopAtShutdown;
    this.extraServlets = extraServlets;
    this.extraFilters = extraFilters;
    this.sslConfig = sslConfig;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(JettyConfig other) {
    Builder builder = new Builder();
    builder.port = other.port;
    builder.context = other.context;
    builder.stopAtShutdown = other.stopAtShutdown;
    builder.extraServlets = other.extraServlets;
    builder.extraFilters = other.extraFilters;
    builder.sslConfig = other.sslConfig;
    return builder;
  }

  public static class Builder {

    int port = 0;
    String context = "/solr";
    boolean stopAtShutdown = true;
    Map<ServletHolder, String> extraServlets = new TreeMap<>();
    Map<Class<? extends Filter>, String> extraFilters = new TreeMap<>();
    SSLConfig sslConfig = null;

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setContext(String context) {
      this.context = context;
      return this;
    }

    public Builder stopAtShutdown(boolean stopAtShutdown) {
      this.stopAtShutdown = stopAtShutdown;
      return this;
    }

    public Builder withServlet(ServletHolder servlet, String servletName) {
      extraServlets.put(servlet, servletName);
      return this;
    }

    public Builder withServlets(Map<ServletHolder, String> servlets) {
      if (servlets != null)
        extraServlets.putAll(servlets);
      return this;
    }

    public Builder withFilter(Class<? extends Filter> filterClass, String filterName) {
      extraFilters.put(filterClass, filterName);
      return this;
    }

    public Builder withFilters(Map<Class<? extends Filter>, String> filters) {
      if (filters != null)
        extraFilters.putAll(filters);
      return this;
    }

    public Builder withSSLConfig(SSLConfig sslConfig) {
      this.sslConfig = sslConfig;
      return this;
    }

    public JettyConfig build() {
      return new JettyConfig(port, context, stopAtShutdown, extraServlets, extraFilters, sslConfig);
    }

  }

}
