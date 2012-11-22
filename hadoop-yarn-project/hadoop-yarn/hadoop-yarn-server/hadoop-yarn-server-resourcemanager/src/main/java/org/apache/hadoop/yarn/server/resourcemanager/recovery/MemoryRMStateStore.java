package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.util.ConverterUtils;

import com.google.common.annotations.VisibleForTesting;


public class MemoryRMStateStore extends RMStateStore {
  
  RMState state = new RMState();
  
  @VisibleForTesting
  public RMState getState() {
    return state;
  }
  
  @Override
  public synchronized RMState loadState() throws Exception {
    // return a copy of the state to allow for modification of the real state
    RMState returnState = new RMState();
    returnState.appState.putAll(state.appState);
    return returnState;
  }
  
  @Override
  public synchronized void initInternal(Configuration conf) {
  }
  
  @Override
  protected synchronized void closeInternal() throws Exception {
  }

  @Override
  public void storeApplicationState(String appId, byte[] appData)
      throws Exception {
    ApplicationStateDataPBImpl appStateData = 
        new ApplicationStateDataPBImpl(
                          ApplicationStateDataProto.parseFrom(appData));
    ApplicationState appState = new ApplicationState(
                         appStateData.getSubmitTime(), 
                         appStateData.getApplicationSubmissionContext());
    state.appState.put(appState.getAppId(), appState);
  }

  @Override
  public synchronized void storeApplicationAttemptState(String attemptIdStr,
      byte[] attemptData) throws Exception {
    ApplicationAttemptId attemptId = ConverterUtils
                                        .toApplicationAttemptId(attemptIdStr);
    ApplicationAttemptStateDataPBImpl attemptStateData = 
                      new ApplicationAttemptStateDataPBImpl(
                      ApplicationAttemptStateDataProto.parseFrom(attemptData));
    ApplicationAttemptState attemptState = new ApplicationAttemptState(
                            attemptId, attemptStateData.getMasterContainer());

    ApplicationState appState = state.getApplicationState().get(
        attemptState.getAttemptId().getApplicationId());
    assert appState != null;

    appState.attempts.put(attemptState.getAttemptId(), attemptState);
  }

  @Override
  public synchronized void removeApplicationState(String appIdStr) 
                                                            throws Exception {
    ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
    assert state.appState.remove(appId) != null;
  }

  @Override
  public synchronized void removeApplicationAttemptState(String attemptIdStr) 
                                                            throws Exception {
    ApplicationAttemptId attemptId = ConverterUtils
                                        .toApplicationAttemptId(attemptIdStr);
    ApplicationState appState = state.getApplicationState().get(
                                                attemptId.getApplicationId());
    if(appState != null) {
      assert appState.attempts.remove(attemptId) != null;
    }
  }

}
