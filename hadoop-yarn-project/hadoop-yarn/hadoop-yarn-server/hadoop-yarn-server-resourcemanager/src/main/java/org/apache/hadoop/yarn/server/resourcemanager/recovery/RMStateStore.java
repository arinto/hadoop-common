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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStoredEvent;

@Private
@Unstable
public abstract class RMStateStore {
  public static final Log LOG = LogFactory.getLog(RMStateStore.class);
  
  public class ApplicationAttemptState {
    final ApplicationAttemptId attemptId;
    final Container masterContainer;
    
    public ApplicationAttemptState(ApplicationAttemptId attemptId,
                                   Container masterContainer) {
      this.attemptId = attemptId;
      this.masterContainer = masterContainer;
    }
    
    public Container getMasterContainer() {
      return masterContainer;
    }
    public ApplicationAttemptId getAttemptId() {
      return attemptId;
    }
  }
  
  public class ApplicationState {
    final ApplicationSubmissionContext context;
    final long submitTime;
    Map<ApplicationAttemptId, ApplicationAttemptState> attempts =
                  new HashMap<ApplicationAttemptId, ApplicationAttemptState>();
    
    ApplicationState(long submitTime, ApplicationSubmissionContext context) {
      this.submitTime = submitTime;
      this.context = context;
    }

    public ApplicationId getAppId() {
      return context.getApplicationId();
    }
    public long getSubmitTime() {
      return submitTime;
    }
    public int getAttemptCount() {
      return attempts.size();
    }
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      return context;
    }
    public ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId) {
      return attempts.get(attemptId);
    }
  }
  
  public class RMState {
    Map<ApplicationId, ApplicationState> appState = 
                                new HashMap<ApplicationId, ApplicationState>();
    
    public Map<ApplicationId, ApplicationState> getApplicationState() {
      return appState;
    }
  }
    
  private Dispatcher rmDispatcher;
  
  public void setDispatcher(Dispatcher dispatcher) {
    this.rmDispatcher = dispatcher;
  }
  
  AsyncDispatcher dispatcher;
  
  public synchronized void init(Configuration conf) throws Exception{    
    // create async handler
    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.register(RMStateStoreEventType.class, 
                        new ForwardingEventHandler());
    dispatcher.start();
    
    initInternal(conf);
  }
  
  protected abstract void initInternal(Configuration conf) throws Exception;
  
  public synchronized void close() throws Exception {
    closeInternal();
    dispatcher.stop();
  }
  
  protected abstract void closeInternal() throws Exception;
  
  public abstract RMState loadState() throws Exception;
  
  /**
   * Blocking API
   */
  public synchronized void storeApplication(RMApp app) throws Exception {
    ApplicationSubmissionContext context = app
                                            .getApplicationSubmissionContext();
    assert context instanceof ApplicationSubmissionContextPBImpl;

    ApplicationStateDataPBImpl appStateData = new ApplicationStateDataPBImpl();
    appStateData.setSubmitTime(app.getSubmitTime());
    appStateData.setApplicationSubmissionContext(context);
    byte[] appStateDataBytes = appStateData.getProto().toByteArray();

    LOG.info("Storing info for app: " + context.getApplicationId());
    storeApplicationState(app.getApplicationId().toString(), appStateDataBytes);
  }
    
  /**
   * Blocking API
   */
  protected abstract void storeApplicationState(String appId,
                                         byte[] appStateData) throws Exception;
  
  @SuppressWarnings("unchecked")
  /**
   * Non-blocking API
   */
  public synchronized void storeApplicationAttempt(RMAppAttempt appAttempt) {
    ApplicationAttemptState attemptState = new ApplicationAttemptState(
                appAttempt.getAppAttemptId(), appAttempt.getMasterContainer());
    dispatcher.getEventHandler().handle(
                                new RMStateStoreAppAttemptEvent(attemptState));
  }
  
  /**
   * Blocking API
   */
  protected abstract void storeApplicationAttemptState(String attemptId,
                                     byte[] attemptStateData) throws Exception;
  
  
  /**
   * Non-blocking API
   */
  public synchronized void removeApplication(RMApp app) {
    ApplicationState appState = new ApplicationState(
        app.getSubmitTime(), app.getApplicationSubmissionContext());
    for(RMAppAttempt appAttempt : app.getAppAttempts().values()) {
      ApplicationAttemptState attemptState = new ApplicationAttemptState(
                appAttempt.getAppAttemptId(), appAttempt.getMasterContainer());
      appState.attempts.put(attemptState.getAttemptId(), attemptState);
    }
    
    removeApplication(appState);
  }
  
  @SuppressWarnings("unchecked")
  /**
   * Non-Blocking API
   */
  public synchronized void removeApplication(ApplicationState appState) {
    dispatcher.getEventHandler().handle(new RMStateStoreRemoveAppEvent(appState));
  }

  /**
   * Blocking API
   */
  protected abstract void removeApplicationState(String appId) throws Exception;
  
  /**
   * Blocking API
   */
  protected abstract void removeApplicationAttemptState(String attemptId)
                                                              throws Exception;

  
  // Dispatcher related code
  
  private synchronized void handleStoreEvent(RMStateStoreEvent event) {
    switch(event.getType()) {
      case STORE_APP_ATTEMPT:
        {
          ApplicationAttemptState attemptState = 
                    ((RMStateStoreAppAttemptEvent) event).getAppAttemptState();
          Exception storedException = null;
          ApplicationAttemptStateDataPBImpl attemptStateData = 
                                        new ApplicationAttemptStateDataPBImpl();
          attemptStateData.setAttemptId(attemptState.getAttemptId());
          attemptStateData.setMasterContainer(attemptState.getMasterContainer());
          byte[] attemptStateDataBytes = 
                                    attemptStateData.getProto().toByteArray();
          LOG.info("Storing info for attempt: " + attemptState.getAttemptId());
          try {
            storeApplicationAttemptState(attemptState.getAttemptId().toString(), 
                                         attemptStateDataBytes);
          } catch (Exception e) {
            LOG.error("Error storing appAttempt: " 
                      + attemptState.getAttemptId(), e);
            storedException = e;
          } finally {
            notifyDoneStoringApplicationAttempt(attemptState.getAttemptId(), 
                                                storedException);            
          }
        }
        break;
      case REMOVE_APP:
        {
          ApplicationState appState = 
                          ((RMStateStoreRemoveAppEvent) event).getAppState();
          ApplicationId appId = appState.getAppId();
          Set<ApplicationAttemptId> attemptIds = appState.attempts.keySet();
          
          LOG.info("Removing info for app: " + appId);
          try {
            removeApplicationState(appId.toString());
          } catch (Exception e) {
            LOG.error("Error removing app: " + appId, e);
          }
          for(ApplicationAttemptId attemptId : attemptIds) {
            LOG.info("Removing info for app: " + appId 
                     + " attempt: " + attemptId);
            try {
              removeApplicationAttemptState(attemptId.toString());
            } catch (Exception e) {
              LOG.error("Error removing info for app: " + appId
                  + " attempt: " + attemptId, e);
            }
          }
        }
        break;
      default:
        LOG.error("Unknown RMStateStoreEvent type: " + event.getType());
    }
  }
  
  @SuppressWarnings("unchecked")
  /**
   * In (@link storeApplicationAttempt}, derived class can call this method to
   * notify the application attempt about operation completion 
   * @param appAttempt attempt that has been saved
   */
  private void notifyDoneStoringApplicationAttempt(ApplicationAttemptId attemptId,
                                                  Exception storedException) {
    rmDispatcher.getEventHandler().handle(
        new RMAppAttemptStoredEvent(attemptId, storedException));
  }
  
  /**
   * EventHandler implementation which forward events to the FSRMStateStore
   * This hides the EventHandle methods of the store from its public interface 
   */
  private final class ForwardingEventHandler 
                                  implements EventHandler<RMStateStoreEvent> {
    
    @Override
    public void handle(RMStateStoreEvent event) {
      handleStoreEvent(event);
    }
    
  }

}