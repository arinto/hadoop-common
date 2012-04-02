/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRetentionPolicy;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerAppStartedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEvent;
import org.apache.hadoop.yarn.service.AbstractService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LogAggregationService extends AbstractService implements
    LogHandler {

  private static final Log LOG = LogFactory
      .getLog(LogAggregationService.class);

  /*
   * Expected deployment TLD will be 1777, owner=<NMOwner>, group=<NMGroup -
   * Group to which NMOwner belongs> App dirs will be created as 750,
   * owner=<AppOwner>, group=<NMGroup>: so that the owner and <NMOwner> can
   * access / modify the files.
   * <NMGroup> should obviously be a limited access group.
   */
  /**
   * Permissions for the top level directory under which app directories will be
   * created.
   */
  private static final FsPermission TLDIR_PERMISSIONS = FsPermission
      .createImmutable((short) 01777);
  /**
   * Permissions for the Application directory.
   */
  private static final FsPermission APP_DIR_PERMISSIONS = FsPermission
      .createImmutable((short) 0750);

  private final Context context;
  private final DeletionService deletionService;
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  Path remoteRootLogDir;
  String remoteRootLogDirSuffix;
  private NodeId nodeId;

  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;

  private final ExecutorService threadPool;

  public LogAggregationService(Dispatcher dispatcher, Context context,
      DeletionService deletionService, LocalDirsHandlerService dirsHandler) {
    super(LogAggregationService.class.getName());
    this.dispatcher = dispatcher;
    this.context = context;
    this.deletionService = deletionService;
    this.dirsHandler = dirsHandler;
    this.appLogAggregators =
        new ConcurrentHashMap<ApplicationId, AppLogAggregator>();
    this.threadPool = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("LogAggregationService #%d")
          .build());
  }

  public synchronized void init(Configuration conf) {
    this.remoteRootLogDir =
        new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));
    this.remoteRootLogDirSuffix =
        conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);

    super.init(conf);
  }

  @Override
  public synchronized void start() {
    // NodeId is only available during start, the following cannot be moved
    // anywhere else.
    this.nodeId = this.context.getNodeId();
    verifyAndCreateRemoteLogDir(getConfig());
    super.start();
  }
  
  @Override
  public synchronized void stop() {
    LOG.info(this.getName() + " waiting for pending aggregation during exit");
    for (AppLogAggregator appLogAggregator : this.appLogAggregators.values()) {
      appLogAggregator.join();
    }
    super.stop();
  }
  
  private void verifyAndCreateRemoteLogDir(Configuration conf) {
    // Checking the existance of the TLD
    FileSystem remoteFS = null;
    try {
      remoteFS = FileSystem.get(conf);
    } catch (IOException e) {
      throw new YarnException("Unable to get Remote FileSystem isntance", e);
    }
    boolean remoteExists = false;
    try {
      remoteExists = remoteFS.exists(this.remoteRootLogDir);
    } catch (IOException e) {
      throw new YarnException("Failed to check for existance of remoteLogDir ["
          + this.remoteRootLogDir + "]");
    }
    if (remoteExists) {
      try {
        FsPermission perms =
            remoteFS.getFileStatus(this.remoteRootLogDir).getPermission();
        if (!perms.equals(TLDIR_PERMISSIONS)) {
          LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
              + "] already exist, but with incorrect permissions. "
              + "Expected: [" + TLDIR_PERMISSIONS + "], Found: [" + perms
              + "]." + " The cluster may have problems with multiple users.");
        }
      } catch (IOException e) {
        throw new YarnException(
            "Failed while attempting to check permissions for dir ["
                + this.remoteRootLogDir + "]");
      }
    } else {
      LOG.warn("Remote Root Log Dir [" + this.remoteRootLogDir
          + "] does not exist. Attempting to create it.");
      try {
        Path qualified =
            this.remoteRootLogDir.makeQualified(remoteFS.getUri(),
                remoteFS.getWorkingDirectory());
        remoteFS.mkdirs(qualified, new FsPermission(TLDIR_PERMISSIONS));
        remoteFS.setPermission(qualified, new FsPermission(TLDIR_PERMISSIONS));
      } catch (IOException e) {
        throw new YarnException("Failed to create remoteLogDir ["
            + this.remoteRootLogDir + "]", e);
      }
    }

  }

  Path getRemoteNodeLogFileForApp(ApplicationId appId, String user) {
    return LogAggregationUtils.getRemoteNodeLogFileForApp(
        this.remoteRootLogDir, appId, user, this.nodeId,
        this.remoteRootLogDirSuffix);
  }

  private void createDir(FileSystem fs, Path path, FsPermission fsPerm)
      throws IOException {
    fs.mkdirs(path, new FsPermission(fsPerm));
    fs.setPermission(path, new FsPermission(fsPerm));
  }

  private void createAppDir(final String user, final ApplicationId appId,
      UserGroupInformation userUgi) {
    try {
      userUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // TODO: Reuse FS for user?
          FileSystem remoteFS = null;
          Path userDir = null;
          Path suffixDir = null;
          Path appDir = null;
          try {
            remoteFS = FileSystem.get(getConfig());
          } catch (IOException e) {
            LOG.error("Failed to get remote FileSystem while processing app "
                + appId, e);
            throw e;
          }
          try {
            userDir =
                LogAggregationUtils.getRemoteLogUserDir(
                    LogAggregationService.this.remoteRootLogDir, user);
            userDir =
                userDir.makeQualified(remoteFS.getUri(),
                    remoteFS.getWorkingDirectory());
            createDir(remoteFS, userDir, APP_DIR_PERMISSIONS);
          } catch (IOException e) {
            LOG.error("Failed to create user dir [" + userDir
                + "] while processing app " + appId);
            throw e;
          }
          try {
            suffixDir =
                LogAggregationUtils.getRemoteLogSuffixedDir(
                    LogAggregationService.this.remoteRootLogDir, user,
                    LogAggregationService.this.remoteRootLogDirSuffix);
            suffixDir =
                suffixDir.makeQualified(remoteFS.getUri(),
                    remoteFS.getWorkingDirectory());
            createDir(remoteFS, suffixDir, APP_DIR_PERMISSIONS);
          } catch (IOException e) {
            LOG.error("Failed to create suffixed user dir [" + suffixDir
                + "] while processing app " + appId);
            throw e;
          }
          try {
            appDir =
                LogAggregationUtils.getRemoteAppLogDir(
                    LogAggregationService.this.remoteRootLogDir, appId, user,
                    LogAggregationService.this.remoteRootLogDirSuffix);
            appDir =
                appDir.makeQualified(remoteFS.getUri(),
                    remoteFS.getWorkingDirectory());
            createDir(remoteFS, appDir, APP_DIR_PERMISSIONS);
          } catch (IOException e) {
            LOG.error("Failed to  create application log dir [" + appDir
                + "] while processing app " + appId);
            throw e;
          }
          return null;
        }
      });
    } catch (Exception e) {
      throw new YarnException(e);
    }
  }

  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, ContainerLogsRetentionPolicy logRetentionPolicy,
      Map<ApplicationAccessType, String> appAcls) {

    // Get user's FileSystem credentials
    UserGroupInformation userUgi =
        UserGroupInformation.createRemoteUser(user);
    if (credentials != null) {
      for (Token<? extends TokenIdentifier> token : credentials
          .getAllTokens()) {
        userUgi.addToken(token);
      }
    }

    // Create the app dir
    createAppDir(user, appId, userUgi);

    // New application
    final AppLogAggregator appLogAggregator =
        new AppLogAggregatorImpl(this.dispatcher, this.deletionService,
            getConfig(), appId, userUgi, dirsHandler,
            getRemoteNodeLogFileForApp(appId, user), logRetentionPolicy,
            appAcls);
    if (this.appLogAggregators.putIfAbsent(appId, appLogAggregator) != null) {
      throw new YarnException("Duplicate initApp for " + appId);
    }


    // TODO Get the user configuration for the list of containers that need log
    // aggregation.

    // Schedule the aggregator.
    Runnable aggregatorWrapper = new Runnable() {
      public void run() {
        try {
          appLogAggregator.run();
        } finally {
          appLogAggregators.remove(appId);
        }
      }
    };
    this.threadPool.execute(aggregatorWrapper);
  }

  // for testing only
  @Private
  int getNumAggregators() {
    return this.appLogAggregators.size();
  }

  private void stopContainer(ContainerId containerId, int exitCode) {

    // A container is complete. Put this containers' logs up for aggregation if
    // this containers' logs are needed.

    if (!this.appLogAggregators.containsKey(
        containerId.getApplicationAttemptId().getApplicationId())) {
      throw new YarnException("Application is not initialized yet for "
          + containerId);
    }
    this.appLogAggregators.get(
        containerId.getApplicationAttemptId().getApplicationId())
        .startContainerLogAggregation(containerId, exitCode == 0);
  }

  private void stopApp(ApplicationId appId) {

    // App is complete. Finish up any containers' pending log aggregation and
    // close the application specific logFile.

    if (!this.appLogAggregators.containsKey(appId)) {
      throw new YarnException("Application is not initialized yet for "
          + appId);
    }
    this.appLogAggregators.get(appId).finishLogAggregation();
  }

  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartEvent =
            (LogHandlerAppStartedEvent) event;
        initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
            appStartEvent.getCredentials(),
            appStartEvent.getLogRetentionPolicy(),
            appStartEvent.getApplicationAcls());
        break;
      case CONTAINER_FINISHED:
        LogHandlerContainerFinishedEvent containerFinishEvent =
            (LogHandlerContainerFinishedEvent) event;
        stopContainer(containerFinishEvent.getContainerId(),
            containerFinishEvent.getExitCode());
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishedEvent =
            (LogHandlerAppFinishedEvent) event;
        stopApp(appFinishedEvent.getApplicationId());
        break;
      default:
        ; // Ignore
    }

  }
}