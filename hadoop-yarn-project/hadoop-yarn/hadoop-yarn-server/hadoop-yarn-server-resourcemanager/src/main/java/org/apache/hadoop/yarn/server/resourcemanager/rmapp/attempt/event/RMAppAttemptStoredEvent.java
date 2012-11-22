package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

public class RMAppAttemptStoredEvent extends RMAppAttemptEvent {

  final Exception storedException;
  
  public RMAppAttemptStoredEvent(ApplicationAttemptId appAttemptId,
                                 Exception storedException) {
    super(appAttemptId, RMAppAttemptEventType.ATTEMPT_SAVED);
    this.storedException = storedException;
  }
  
  public Exception getStoredException() {
    return storedException;
  }

}
