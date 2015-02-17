package org.apache.solr.handler;

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
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.ConfigOverlay;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.PluginsRegistry;
import org.apache.solr.core.RequestParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.transform.TransformerFactory;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ValueSourceParser;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.text.MessageFormat.format;
import static java.util.Collections.singletonList;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.core.ConfigOverlay.NOT_EDITABLE;
import static org.apache.solr.core.SolrConfig.PluginOpts.REQUIRE_NAME;
import static org.apache.solr.schema.FieldType.CLASS_NAME;

public class SolrConfigHandler extends RequestHandlerBase {
  public static final Logger log = LoggerFactory.getLogger(SolrConfigHandler.class);
  public static final boolean configEditing_disabled = Boolean.getBoolean("disable.configEdit");
  private static final Map<String, SolrConfig.SolrPluginInfo> namedPlugins;

  static {
    Map<String, SolrConfig.SolrPluginInfo> map = new HashMap<>();
    for (SolrConfig.SolrPluginInfo plugin : SolrConfig.plugins) {
      if (plugin.options.contains(REQUIRE_NAME)) {
        map.put(plugin.tag.toLowerCase(Locale.ROOT), plugin);

      }
    }
    namedPlugins = Collections.unmodifiableMap(map);
  }


  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    setWt(req, "json");
    String httpMethod = (String) req.getContext().get("httpMethod");
    Command command = new Command(req, rsp, httpMethod);
    if ("POST".equals(httpMethod)) {
      if (configEditing_disabled)
        throw new SolrException(SolrException.ErrorCode.FORBIDDEN, " solrconfig editing is not enabled");
      command.handlePOST();
    } else {
      command.handleGET();
    }
  }


  private static class Command {
    private final SolrQueryRequest req;
    private final SolrQueryResponse resp;
    private final String method;
    private String path;
    List<String> parts;

    private Command(SolrQueryRequest req, SolrQueryResponse resp, String httpMethod) {
      this.req = req;
      this.resp = resp;
      this.method = httpMethod;
      path = (String) req.getContext().get("path");
      if (path == null) path = getDefaultPath();
      parts = StrUtils.splitSmart(path, '/');
      if (parts.get(0).isEmpty()) parts.remove(0);
    }

    private String getDefaultPath() {
      return "/config";
    }

    private void handleGET() {
      if (parts.size() == 1) {
        resp.add("config", getConfigDetails());
      } else {
        if (ConfigOverlay.NAME.equals(parts.get(1))) {
          resp.add(ConfigOverlay.NAME, req.getCore().getSolrConfig().getOverlay().toMap());
        } else if (RequestParams.NAME.equals(parts.get(1))) {
          if (parts.size() == 3) {
            RequestParams params = req.getCore().getSolrConfig().getRequestParams();
            MapSolrParams p = params.getParams(parts.get(2));
            Map m = new LinkedHashMap<>();
            m.put(ConfigOverlay.ZNODEVER, params.getZnodeVersion());
            if (p != null) {
              m.put(RequestParams.NAME, ZkNodeProps.makeMap(parts.get(2), p.getMap()));
            }
            resp.add(SolrQueryResponse.NAME, m);
          } else {
            resp.add(SolrQueryResponse.NAME, req.getCore().getSolrConfig().getRequestParams().toMap());
          }

        } else {
          Map<String, Object> m = getConfigDetails();
          resp.add("config", ZkNodeProps.makeMap(parts.get(1), m.get(parts.get(1))));
        }
      }
    }

    private Map<String, Object> getConfigDetails() {
      Map<String, Object> map = req.getCore().getSolrConfig().toMap();
      Map reqHandlers = (Map) map.get(SolrRequestHandler.TYPE);
      if (reqHandlers == null) map.put(SolrRequestHandler.TYPE, reqHandlers = new LinkedHashMap<>());
      List<PluginInfo> plugins = PluginsRegistry.getHandlers(req.getCore());
      for (PluginInfo plugin : plugins) {
        if (SolrRequestHandler.TYPE.equals(plugin.type)) {
          if (!reqHandlers.containsKey(plugin.name)) {
            reqHandlers.put(plugin.name, plugin.toMap());
          }
        }
      }
      return map;
    }


    private void handlePOST() throws IOException {
      Iterable<ContentStream> streams = req.getContentStreams();
      if (streams == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing content stream");
      }
      ArrayList<CommandOperation> ops = new ArrayList<>();

      for (ContentStream stream : streams)
        ops.addAll(CommandOperation.parse(stream.getReader()));
      List<Map> errList = CommandOperation.captureErrors(ops);
      if (!errList.isEmpty()) {
        resp.add(CommandOperation.ERR_MSGS, errList);
        return;
      }

      try {
        for (; ; ) {
          ArrayList<CommandOperation> opsCopy = new ArrayList<>(ops.size());
          for (CommandOperation op : ops) opsCopy.add(op.getCopy());
          try {
            if (parts.size() > 1 && RequestParams.NAME.equals(parts.get(1))) {
              RequestParams params = RequestParams.getFreshRequestParams(req.getCore().getResourceLoader(), req.getCore().getSolrConfig().getRequestParams());
              handleParams(opsCopy, params);
            } else {
              ConfigOverlay overlay = SolrConfig.getConfigOverlay(req.getCore().getResourceLoader());
              handleCommands(opsCopy, overlay);
            }
            break;//succeeded . so no need to go over the loop again
          } catch (ZkController.ResourceModifiedInZkException e) {
            //retry
            log.info("Race condition, the node is modified in ZK by someone else " + e.getMessage());
          }
        }
      } catch (Exception e) {
        resp.setException(e);
        resp.add(CommandOperation.ERR_MSGS, singletonList(SchemaManager.getErrorStr(e)));
      }

    }


    private void handleParams(ArrayList<CommandOperation> ops, RequestParams params) {
      for (CommandOperation op : ops) {
        switch (op.name) {
          case SET:
          case UPDATE: {
            Map<String, Object> map = op.getDataMap();
            if (op.hasError()) break;

            for (Map.Entry<String, Object> entry : map.entrySet()) {

              Map val = null;
              String key = entry.getKey();
              if (key == null || key.trim().isEmpty()) {
                op.addError("null key ");
                continue;
              }
              key = key.trim();
              String err = validateName(key);
              if (err != null) {
                op.addError(err);
                continue;
              }

              try {
                val = (Map) entry.getValue();
              } catch (Exception e1) {
                op.addError("invalid params for key : " + key);
                continue;
              }

              if (val.containsKey("")) {
                op.addError("Empty keys are not allowed in params");
                continue;
              }

              MapSolrParams old = params.getParams(key);
              if (op.name.equals(UPDATE)) {
                LinkedHashMap m = new LinkedHashMap(old.getMap());
                m.putAll(val);
                val = m;
              }
              params = params.setParams(key, val);

            }
            break;

          }
          case "delete": {
            List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
            if (op.hasError()) break;
            for (String s : name) {
              if (params.getParams(s) == null) {
                op.addError(MessageFormat.format("can't delete . No such params ''{0}'' exist", s));
              }
              params = params.setParams(s, null);
            }
          }
        }
      }


      List errs = CommandOperation.captureErrors(ops);
      if (!errs.isEmpty()) {
        resp.add(CommandOperation.ERR_MSGS, errs);
        return;
      }

      SolrResourceLoader loader = req.getCore().getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
        if (ops.isEmpty()) {
          ZkController.touchConfDir(zkLoader);
        } else {
          ZkController.persistConfigResourceToZooKeeper(zkLoader, params.getZnodeVersion(),
              RequestParams.RESOURCE, params.toByteArray(), true);
        }

      } else {
        SolrResourceLoader.persistConfLocally(loader, RequestParams.RESOURCE, params.toByteArray());
        req.getCore().getSolrConfig().refreshRequestParams();
      }

    }

    private void handleCommands(List<CommandOperation> ops, ConfigOverlay overlay) throws IOException {
      for (CommandOperation op : ops) {
        switch (op.name) {
          case SET_PROPERTY:
            overlay = applySetProp(op, overlay);
            break;
          case UNSET_PROPERTY:
            overlay = applyUnset(op, overlay);
            break;
          case SET_USER_PROPERTY:
            overlay = applySetUserProp(op, overlay);
            break;
          case UNSET_USER_PROPERTY:
            overlay = applyUnsetUserProp(op, overlay);
            break;
          default: {
            List<String> pcs = StrUtils.splitSmart(op.name.toLowerCase(Locale.ROOT), '-');
            if (pcs.size() != 2) {
              op.addError(MessageFormat.format("Unknown operation ''{0}'' ", op.name));
            } else {
              String prefix = pcs.get(0);
              String name = pcs.get(1);
              if (cmdPrefixes.contains(prefix) && namedPlugins.containsKey(name)) {
                SolrConfig.SolrPluginInfo info = namedPlugins.get(name);
                if ("delete".equals(prefix)) {
                  overlay = deleteNamedComponent(op, overlay, info.tag);
                } else {
                  overlay = updateNamedPlugin(info, op, overlay, prefix.equals("create"));
                }
              } else {
                op.addError(MessageFormat.format("Unknown operation ''{0}'' ", op.name));
              }
            }
          }
        }
      }
      List errs = CommandOperation.captureErrors(ops);
      if (!errs.isEmpty()) {
        log.info("Failed to run commands errors are {}", StrUtils.join(errs, ','));
        resp.add(CommandOperation.ERR_MSGS, errs);
        return;
      }

      SolrResourceLoader loader = req.getCore().getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        ZkController.persistConfigResourceToZooKeeper((ZkSolrResourceLoader) loader, overlay.getZnodeVersion(),
            ConfigOverlay.RESOURCE_NAME, overlay.toByteArray(), true);

        log.info("Executed config commands successfully and persited to ZK {}", ops);
      } else {
        SolrResourceLoader.persistConfLocally(loader, ConfigOverlay.RESOURCE_NAME, overlay.toByteArray());
        req.getCore().getCoreDescriptor().getCoreContainer().reload(req.getCore().getName());
        log.info("Executed config commands successfully and persited to File System {}", ops);
      }

    }

    private ConfigOverlay deleteNamedComponent(CommandOperation op, ConfigOverlay overlay, String typ) {
      String name = op.getStr(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;
      if (overlay.getNamedPlugins(typ).containsKey(name)) {
        return overlay.deleteNamedPlugin(name, typ);
      } else {
        op.addError(MessageFormat.format("NO such {0} ''{1}'' ", typ, name));
        return overlay;
      }
    }

    private ConfigOverlay updateNamedPlugin(SolrConfig.SolrPluginInfo info, CommandOperation op, ConfigOverlay overlay, boolean isCeate) {
      String name = op.getStr(NAME);
      String clz = op.getStr(CLASS_NAME);
      op.getMap(PluginInfo.DEFAULTS, null);
      op.getMap(PluginInfo.INVARIANTS, null);
      op.getMap(PluginInfo.APPENDS, null);
      if (op.hasError()) return overlay;
      if (!verifyClass(op, clz, info.clazz)) return overlay;
      if (overlay.getNamedPlugins(info.tag).containsKey(name)) {
        if (isCeate) {
          op.addError(MessageFormat.format(" ''{0}'' already exists . Do an ''{1}'' , if you want to change it ", name, "update-" + info.tag.toLowerCase(Locale.ROOT)));
          return overlay;
        } else {
          return overlay.addNamedPlugin(op.getDataMap(), info.tag);
        }
      } else {
        if (isCeate) {
          return overlay.addNamedPlugin(op.getDataMap(), info.tag);
        } else {
          op.addError(MessageFormat.format(" ''{0}'' does not exist . Do an ''{1}'' , if you want to create it ", name, "create-" + info.tag.toLowerCase(Locale.ROOT)));
          return overlay;
        }
      }
    }

    private boolean verifyClass(CommandOperation op, String clz, Class expected) {
      if (op.getStr("lib", null) == null) {
        //this is not dynamically loaded so we can verify the class right away
        try {
          SolrCore.createInstance(clz, expected, expected.getSimpleName(), req.getCore());
        } catch (Exception e) {
          op.addError(e.getMessage());
          return false;
        }

      }
      return true;
    }

    private ConfigOverlay applySetUserProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if (op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        overlay = overlay.setUserProperty(name, val);
      }
      return overlay;
    }

    private ConfigOverlay applyUnsetUserProp(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;
      for (String o : name) {
        if (!overlay.getUserProps().containsKey(o)) {
          op.addError(format("No such property ''{0}''", name));
        } else {
          overlay = overlay.unsetUserProperty(o);
        }
      }
      return overlay;
    }


    private ConfigOverlay applyUnset(CommandOperation op, ConfigOverlay overlay) {
      List<String> name = op.getStrs(CommandOperation.ROOT_OBJ);
      if (op.hasError()) return overlay;

      for (String o : name) {
        if (!ConfigOverlay.isEditableProp(o, false, null)) {
          op.addError(format(NOT_EDITABLE, name));
        } else {
          overlay = overlay.unsetProperty(o);
        }
      }
      return overlay;
    }

    private ConfigOverlay applySetProp(CommandOperation op, ConfigOverlay overlay) {
      Map<String, Object> m = op.getDataMap();
      if (op.hasError()) return overlay;
      for (Map.Entry<String, Object> e : m.entrySet()) {
        String name = e.getKey();
        Object val = e.getValue();
        if (!ConfigOverlay.isEditableProp(name, false, null)) {
          op.addError(format(NOT_EDITABLE, name));
          continue;
        }
        overlay = overlay.setProperty(name, val);
      }
      return overlay;
    }

  }

  public static String validateName(String s) {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if ((c >= 'A' && c <= 'Z') ||
          (c >= 'a' && c <= 'z') ||
          (c >= '0' && c <= '9') ||
          c == '_' ||
          c == '-' ||
          c == '.'
          ) continue;
      else {
        return MessageFormat.format("''{0}'' name should only have chars [a-zA-Z_-.0-9] ", s);
      }
    }
    return null;
  }

  static void setWt(SolrQueryRequest req, String wt) {
    SolrParams params = req.getParams();
    if (params.get(CommonParams.WT) != null) return;//wt is set by user
    Map<String, String> map = new HashMap<>(1);
    map.put(CommonParams.WT, wt);
    map.put("indent", "true");
    req.setParams(SolrParams.wrapDefaults(params, new MapSolrParams(map)));
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if (subPaths.contains(path)) return this;
    if (path.startsWith("/params/")) return this;
    return null;
  }


  private static Set<String> subPaths = new HashSet<>(Arrays.asList("/overlay", "/params",
      "/query", "/jmx", "/requestDispatcher"));

  static {
    for (SolrConfig.SolrPluginInfo solrPluginInfo : SolrConfig.plugins)
      subPaths.add("/" + solrPluginInfo.tag.replaceAll("/", ""));

  }

  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getDescription() {
    return "Edit solrconfig.xml";
  }


  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }


  public static final String SET_PROPERTY = "set-property";
  public static final String UNSET_PROPERTY = "unset-property";
  public static final String SET_USER_PROPERTY = "set-user-property";
  public static final String UNSET_USER_PROPERTY = "unset-user-property";
  public static final String SET = "set";
  public static final String UPDATE = "update";
  public static final String CREATE = "create";
  private static Set<String> cmdPrefixes = ImmutableSet.of(CREATE, UPDATE, "delete");

}
