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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAZKUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class ZKRMStateStore extends RMStateStore {
  
  public static final Log LOG = LogFactory.getLog(ZKRMStateStore.class);
  
  private static final int NUM_RETRIES = 3;
  private static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";

  
  private ZooKeeper zkClient;
  private ZooKeeper oldZkClient;
  
  private String zkHostPort;
  private int zkSessionTimeout;
  private List<ACL> zkAcl;
  private String zkRootNodePath;

  @VisibleForTesting
  protected String znodeWorkingPath;

  public synchronized void initInternal(Configuration conf) throws Exception {
    znodeWorkingPath = conf.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH
                    , YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH);
    zkHostPort = conf.get(YarnConfiguration.ZK_RM_STATE_STORE_ADDRESS);
    zkSessionTimeout = conf.getInt(YarnConfiguration.ZK_RM_STATE_STORE_TIMEOUT_MS,
                        YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_TIMEOUT_MS);
    // Parse authentication from configuration.
    String zkAclConf = conf.get(YarnConfiguration.ZK_RM_STATE_STORE_ACL, 
                              YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_ACL);
    zkAclConf = HAZKUtil.resolveConfIndirection(zkAclConf);
    List<ACL> zkConfAcls = HAZKUtil.parseACLs(zkAclConf);
    if (zkConfAcls.isEmpty()) {
      zkConfAcls = Ids.OPEN_ACL_UNSAFE;
    }
    zkAcl = zkConfAcls;

    zkRootNodePath = znodeWorkingPath + "/" + ROOT_ZNODE_NAME;
    
    // createConnection for future API calls
    createConnection();    

    // ensure root dir exists
    try {
      createWithRetries(zkRootNodePath, null, zkAcl, CreateMode.PERSISTENT);
    } catch (KeeperException ke) {
      if(ke.code() != Code.NODEEXISTS) {
        throw ke;
      }
    }    
  }
  
  @Override
  protected synchronized void closeInternal() throws Exception {
    zkClient.close();
  }

  @Override
  public synchronized RMState loadState() throws Exception {
    try {
      RMState state = new RMState();
      List<String> childNodes = zkClient.getChildren(zkRootNodePath, false);
      List<ApplicationAttemptState> attempts = 
                                      new ArrayList<ApplicationAttemptState>();
      for(String childNodeName : childNodes) {
        String childNodePath = getNodePath(childNodeName);
        byte[] childData = getDataWithRetries(childNodePath, false);
        if(childNodeName.startsWith(ApplicationId.appIdStrPrefix)){
          // application
          LOG.info("Loading application from znode: " + childNodeName);
          ApplicationId appId = ConverterUtils.toApplicationId(childNodeName);
          ApplicationStateDataPBImpl appStateData = 
              new ApplicationStateDataPBImpl(
                                ApplicationStateDataProto.parseFrom(childData));
          ApplicationState appState = new ApplicationState(
                               appStateData.getSubmitTime(), 
                               appStateData.getApplicationSubmissionContext());
          // assert child node name is same as actual applicationId
          assert appId.equals(appState.context.getApplicationId());
          state.appState.put(appId, appState);
        } else if(childNodeName.startsWith(
                                ApplicationAttemptId.appAttemptIdStrPrefix)) {
          // attempt
          LOG.info("Loading application attempt from znode: " + childNodeName);
          ApplicationAttemptId attemptId = 
                          ConverterUtils.toApplicationAttemptId(childNodeName);
          ApplicationAttemptStateDataPBImpl attemptStateData = 
              new ApplicationAttemptStateDataPBImpl(
                  ApplicationAttemptStateDataProto.parseFrom(childData));
          ApplicationAttemptState attemptState = new ApplicationAttemptState(
                            attemptId, attemptStateData.getMasterContainer());
          // assert child node name is same as application attempt id
          assert attemptId.equals(attemptState.getAttemptId());
          attempts.add(attemptState);
        } else {
          LOG.info("Unknown child node with name: " + childNodeName);
        }
      }
      
      // go through all attempts and add them to their apps
      for(ApplicationAttemptState attemptState : attempts) {
        ApplicationId appId = attemptState.getAttemptId().getApplicationId();
        ApplicationState appState = state.appState.get(appId);
        if(appState != null) {
          appState.attempts.put(attemptState.getAttemptId(), attemptState);
        } else {
          // the application znode may have been removed when the application 
          // completed but the RM might have stopped before it could remove the 
          // application attempt znodes
          LOG.info("Application node not found for attempt: " 
                    + attemptState.getAttemptId());
          deleteWithRetries(
                      getNodePath(attemptState.getAttemptId().toString()), 0);
        }
      }
      
      return state;
    } catch (Exception e) {
      LOG.error("Failed to load state.", e);
      throw e;
    }
  }
  
  @Override
  public synchronized void storeApplicationState(String appId,
                                    ApplicationStateDataPBImpl appStateDataPB) 
                                    throws Exception {
    String nodeCreatePath = getNodePath(appId);
    
    LOG.info("Storing info for app: " + appId + " at: " + nodeCreatePath);
    byte[] appStateData = appStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA 
      // based on whether we have lost the right to write to ZK
      createWithRetries(nodeCreatePath, appStateData, 
                        zkAcl, CreateMode.PERSISTENT);
    } catch (Exception e) {
      LOG.info("Error storing info for app: " + appId, e);
      throw e;
    }
  }

  @Override
  public synchronized void storeApplicationAttemptState(String attemptId,
                          ApplicationAttemptStateDataPBImpl attemptStateDataPB) 
                          throws Exception {
    String nodeCreatePath = getNodePath(attemptId);
    LOG.info("Storing info for attempt: " + attemptId
             + " at: " + nodeCreatePath);
    byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
    try {
      // currently throw all exceptions. May need to respond differently for HA 
      // based on whether we have lost the right to write to ZK
      createWithRetries(nodeCreatePath, attemptStateData, 
                        zkAcl, CreateMode.PERSISTENT);
    } catch (Exception e) {
      LOG.info("Error storing info for attempt: " + attemptId, e);
      throw e;
    }
  }

  @Override
  public synchronized void removeApplicationState(ApplicationState appState) 
                                                            throws Exception {
    String appId = appState.getAppId().toString();
    String nodeRemovePath = getNodePath(appId);
    LOG.info("Removing info for app: " + appId + " at: " + nodeRemovePath);
    try {
      deleteWithRetries(nodeRemovePath, 0);
    } catch (Exception e) {
      LOG.error("Error removing info for app: " + appId, e);
    }
    
    for(ApplicationAttemptId attemptId : appState.attempts.keySet()) {
      removeApplicationAttemptState(attemptId.toString());
    }
  }
  
  public synchronized void removeApplicationAttemptState(String attemptId) 
                                                            throws Exception {
    String nodeRemovePath = getNodePath(attemptId);
    LOG.info("Removing info for attempt: " + attemptId 
             + " at: " + nodeRemovePath);
    try {
      deleteWithRetries(nodeRemovePath, 0);
    } catch (Exception e) {
      LOG.error("Error removing info for app: " + attemptId, e);
    }
  }
      
  // ZK related code
  /**
   * Watcher implementation which forward events to the ZKRMStateStore
   * This hides the ZK methods of the store from its public interface 
   */
  private final class ForwardingWatcher implements Watcher {
    ZooKeeper zk;
    public ForwardingWatcher(ZooKeeper zk) {
      this.zk = zk;
    }
    @Override
    public void process(WatchedEvent event) {
      try {
        ZKRMStateStore.this.processWatchEvent(zk, event);
      } catch (Throwable t) {
        LOG.error("Failed to process watcher event " + event + ": " +
                  StringUtils.stringifyException(t));
      }
    }
  }
  
  private synchronized void processWatchEvent(ZooKeeper zk, WatchedEvent event) 
                                                             throws Exception {
    Event.EventType eventType = event.getType();
    LOG.info("Watcher event type: " + eventType + " with state:"
        + event.getState() + " for path:" + event.getPath()
        + " for " + this);

    if (eventType == Event.EventType.None) {
      // the connection state has changed
      switch (event.getState()) {
      case SyncConnected:
        LOG.info("ZKRMStateStore Session connected");
        if(oldZkClient != null) {
          // the SyncConnected must be from the client that sent Disconnected
          assert oldZkClient == zk;
          zkClient = oldZkClient;
          oldZkClient = null;
          ZKRMStateStore.this.notifyAll();
          LOG.info("ZKRMStateStore Session restored");
        }
        break;
      case Disconnected:
        LOG.info("ZKRMStateStore Session disconnected");
        oldZkClient = zkClient;
        zkClient = null;
        break;
      case Expired:
        // the connection got terminated because of session timeout
        // call listener to reconnect
        LOG.info("Session expired");
        createConnection();
        break;
      default:
        LOG.error("Unexpected Zookeeper watch event state: " + event.getState());
        break;
      }
    }
  }
  
  @VisibleForTesting
  String getNodePath(String nodeName) {
    return (zkRootNodePath + "/" + nodeName);
  }
  
  private String createWithRetries(final String path, final byte[] data,
      final List<ACL> acl, final CreateMode mode)
      throws Exception {
    return zkDoWithRetries(new ZKAction<String>() {
      @Override
      public String run() throws KeeperException, InterruptedException {
        return zkClient.create(path, data, acl, mode);
      }
    });
  }

  private void deleteWithRetries(final String path, final int version)
      throws Exception {
    zkDoWithRetries(new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        zkClient.delete(path, version);
        return null;
      }
    });
  }
  
  private byte[] getDataWithRetries(final String path, final boolean watch) 
                                throws Exception {
    return zkDoWithRetries(new ZKAction<byte[]>() {
      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        return zkClient.getData(path, watch, stat);
      }
    });
  }

  private static <T> T zkDoWithRetries(ZKAction<T> action)
      throws Exception {
    int retry = 0;
    while (true) {
      try {
        return action.runWithCheck();
      } catch (KeeperException ke) {
        if (shouldRetry(ke.code()) && ++retry < NUM_RETRIES) {
          continue;
        }
        throw ke;
      }
    }
  }
  
  private abstract class ZKAction<T> {
    abstract T run() throws KeeperException, InterruptedException; 
    T runWithCheck() throws Exception {
      long startTime = System.currentTimeMillis();
      while(zkClient == null) {
        ZKRMStateStore.this.wait(zkSessionTimeout);
        if(zkClient != null) {
          break;
        }
        if(System.currentTimeMillis()-startTime > zkSessionTimeout) {
          throw new Exception("Wait for ZKClient creation timed out");
        }
      }
      return run();          
    }
  }
  
  private static boolean shouldRetry(Code code) {
    switch (code) {
    case CONNECTIONLOSS:
    case OPERATIONTIMEOUT:
      return true;
    }
    return false;
  }

  private void createConnection() throws Exception {
    if (zkClient != null) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing ZK", e);
      }
      zkClient = null;
    }
    if(oldZkClient != null) {
      try {
        oldZkClient.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing old ZK", e);
      }
      oldZkClient = null;      
    }
    zkClient = getNewZooKeeper();
    ZKRMStateStore.this.notifyAll();
    LOG.info("Created new connection for " + this);
  }
  
  // protected to mock for testing
  protected synchronized ZooKeeper getNewZooKeeper() throws Exception {
    ZooKeeper zk = new ZooKeeper(zkHostPort, zkSessionTimeout, null);
    zk.register(new ForwardingWatcher(zk));
    return zk;
  }
}
