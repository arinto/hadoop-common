package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NdbRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestNDBRMRestart {
  
  @Test
  public void testRMRestart() throws Exception {
    Logger rootLogger = LogManager.getRootLogger();
    rootLogger.setLevel(Level.DEBUG);
    YarnConfiguration conf = new YarnConfiguration();
    conf.set(YarnConfiguration.NDB_RM_STATE_STORE_CONFIG_PATH,
            "src/test/java/org/apache/hadoop/yarn/server/resourcemanager/clusterj.properties");
    conf.set(YarnConfiguration.RM_SCHEDULER, 
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");

    NdbRMStateStore ndbStore = new NdbRMStateStore();
    ndbStore.init(conf);
    //RMstate rmState = ndbStore.getState(); Remove this because we need to load it from db, not memory
    
    /*
     * MemoryRMStateStore memStore = new MemoryRMStateStore(); now to NDB!
    memStore.init(conf);
    RMState rmState = memStore.getState();*/
   
    //Map<ApplicationId, ApplicationState> rmAppState = rmState.getApplicationState(); now loads from DD!
    //Map<ApplicationId, ApplicationState> rmAppState = ndbStore.loadState().getApplicationState();
    
    // PHASE 1: create state in an RM
    
    // start RM
    MockRM rm1 = new MockRM(conf, true, ndbStore);
    
    // start like normal because state is empty
    rm1.start();
    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    MockNM nm2 = new MockNM("h2:5678", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm2.registerNode(); // nm2 will not heartbeat with RM1
    
    // create app that will not be saved because it will finish
    RMApp app0 = rm1.submitApp(200);
    RMAppAttempt attempt0 = app0.getCurrentAppAttempt();
    
    // spot check that app is saved
    Assert.assertEquals(1, ndbStore.loadState().getApplicationState().size());
    nm1.nodeHeartbeat(true);
    MockAM am0 = rm1.sendAMLaunched(attempt0.getAppAttemptId());
    am0.registerAppAttempt();
    am0.unregisterAppAttempt();
    nm1.nodeHeartbeat(attempt0.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am0.waitForState(RMAppAttemptState.FINISHED);    

    // spot check that app is not saved anymore
    Assert.assertEquals(0, ndbStore.loadState().getApplicationState().size());
        
    // create app that gets launched and does allocate before RM restart
    RMApp app1 = rm1.submitApp(200);
    // assert app1 info is saved
    ApplicationState appState = ndbStore.loadState().getApplicationState().get(app1.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app1.getApplicationSubmissionContext()
        .getApplicationId());

    //kick the scheduling to allocate AM container
    nm1.nodeHeartbeat(true);
    
    // assert app1 attempt is saved
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
    rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    
    appState = ndbStore.loadState().getApplicationState().get(app1.getApplicationId());
    Assert.assertEquals(1, appState.getAttemptCount());
    ApplicationAttemptState attemptState = 
                                appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());
    
    // launch the AM
    MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // AM request for containers
    am1.allocate("h1" , 1000, 1, new ArrayList<ContainerId>());    
    // kick the scheduler
    nm1.nodeHeartbeat(true);
    List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }
    
    // create app that does not get launched by RM before RM restart
    RMApp app2 = rm1.submitApp(200);

    // assert app2 info is saved
    appState = ndbStore.loadState().getApplicationState().get(app2.getApplicationId());
    Assert.assertNotNull(appState);
    Assert.assertEquals(0, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), app2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // create unmanaged app
    RMApp appUnmanaged = rm1.submitApp(200, "", "", null, true);
    ApplicationAttemptId unmanagedAttemptId = 
                        appUnmanaged.getCurrentAppAttempt().getAppAttemptId();
    // assert appUnmanaged info is saved
    appState = ndbStore.loadState().getApplicationState().get(appUnmanaged.getApplicationId());
    Assert.assertNotNull(appState);
    // wait for attempt to reach LAUNCHED state 
    rm1.waitForState(unmanagedAttemptId, RMAppAttemptState.LAUNCHED);
    // assert unmanaged attempt info is saved
    appState = ndbStore.loadState().getApplicationState().get(appUnmanaged.getApplicationId());
    Assert.assertEquals(1, appState.getAttemptCount());
    Assert.assertEquals(appState.getApplicationSubmissionContext()
        .getApplicationId(), appUnmanaged.getApplicationSubmissionContext()
        .getApplicationId());
    
    
    
    // PHASE 2: create new RM and start from old state
    
    // create new RM to represent restart
    MockRM rm2 = new MockRM(conf, true, ndbStore);
    
    // recover state
    rm2.recover(ndbStore.loadState());
    
    // change NM to point to new RM
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    nm2.setResourceTrackerService(rm2.getResourceTrackerService());

    // verify load of old state
    // only 2 apps are loaded since unmanaged app is not loaded back since it
    // cannot be restarted by the RM this will change with work preserving RM
    // restart in which AMs/NMs are not rebooted
    Assert.assertEquals(2, rm2.getRMContext().getRMApps().size());
    
    // verify correct number of attempts and other data
    RMApp loadedApp1 = rm2.getRMContext().getRMApps().get(app1.getApplicationId());
    Assert.assertNotNull(loadedApp1);
    Assert.assertEquals(1, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(app1.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp1.getApplicationSubmissionContext()
        .getApplicationId());
    
    RMApp loadedApp2 = rm2.getRMContext().getRMApps().get(app2.getApplicationId());
    Assert.assertNotNull(loadedApp2);
    Assert.assertEquals(0, loadedApp2.getAppAttempts().size());
    Assert.assertEquals(app2.getApplicationSubmissionContext()
        .getApplicationId(), loadedApp2.getApplicationSubmissionContext()
        .getApplicationId());
    
    // start new RM
    rm2.start();
    
    // verify state machine kicked into expected states
    rm2.waitForState(loadedApp1.getApplicationId(), RMAppState.ACCEPTED);
    rm2.waitForState(loadedApp2.getApplicationId(), RMAppState.ACCEPTED);
    
    // verify new attempts created
    Assert.assertEquals(2, loadedApp1.getAppAttempts().size());
    Assert.assertEquals(1, loadedApp2.getAppAttempts().size());
    
    // verify old AM is not accepted
    // change running AM to talk to new RM
    am1.setAMRMProtocol(rm2.getApplicationMasterService());
    AMResponse amResponse = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>());
    Assert.assertTrue(amResponse.getReboot());
    
    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    HeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.REBOOT, hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.REBOOT, hbResponse.getNodeAction());
    
    // new NM to represent NM re-register
    nm1 = rm2.registerNode("h1:1234", 15120);
    nm2 = rm2.registerNode("h2:5678", 15120);

    // verify no more reboot response sent
    hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.REBOOT != hbResponse.getNodeAction());
    hbResponse = nm2.nodeHeartbeat(true);
    Assert.assertTrue(NodeAction.REBOOT != hbResponse.getNodeAction());
    
    // assert app1 attempt is saved
    attempt1 = loadedApp1.getCurrentAppAttempt();
    attemptId1 = attempt1.getAppAttemptId();
    rm2.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
    appState = ndbStore.loadState().getApplicationState().get(loadedApp1.getApplicationId());
    attemptState = appState.getAttempt(attemptId1);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1), 
                        attemptState.getMasterContainer().getId());

    // Nodes on which the AM's run 
    MockNM am1Node = nm1;
    if(attemptState.getMasterContainer().getNodeId().toString().contains("h2")){
      am1Node = nm2;
    }

    // assert app2 attempt is saved
    RMAppAttempt attempt2 = loadedApp2.getCurrentAppAttempt();
    ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
    rm2.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);
    appState = ndbStore.loadState().getApplicationState().get(loadedApp2.getApplicationId());
    attemptState = appState.getAttempt(attemptId2);
    Assert.assertNotNull(attemptState);
    Assert.assertEquals(BuilderUtils.newContainerId(attemptId2, 1), 
                        attemptState.getMasterContainer().getId());

    MockNM am2Node = nm1;
    if(attemptState.getMasterContainer().getNodeId().toString().contains("h2")){
      am2Node = nm2;
    }
    
    // start the AM's
    am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();
    
    MockAM am2 = rm2.sendAMLaunched(attempt2.getAppAttemptId());
    am2.registerAppAttempt();

    //request for containers
    am1.allocate("h1" , 1000, 3, new ArrayList<ContainerId>());
    
    // verify container allocate continues to work
    nm1.nodeHeartbeat(true);
    nm2.nodeHeartbeat(true);
    conts = am1.allocate(new ArrayList<ResourceRequest>(),
        new ArrayList<ContainerId>()).getAllocatedContainers();
    while (conts.size() == 0) {
      conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
          new ArrayList<ContainerId>()).getAllocatedContainers());
      Thread.sleep(500);
    }

    // finish the AM's
    am1.unregisterAppAttempt();
    am1Node.nodeHeartbeat(attempt1.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am1.waitForState(RMAppAttemptState.FINISHED);
    
    am2.unregisterAppAttempt();
    am2Node.nodeHeartbeat(attempt2.getAppAttemptId(), 1, ContainerState.COMPLETE);
    am2.waitForState(RMAppAttemptState.FINISHED);
    
    // stop RM's
    rm2.stop();
    rm1.stop();
    
    // completed apps should be removed
    Assert.assertEquals(0, ndbStore.loadState().getApplicationState().size());
 }
  
}
