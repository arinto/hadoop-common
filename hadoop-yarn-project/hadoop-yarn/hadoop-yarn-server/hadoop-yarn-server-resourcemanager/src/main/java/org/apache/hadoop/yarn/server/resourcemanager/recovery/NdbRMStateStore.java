/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import com.google.protobuf.InvalidProtocolBufferException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.SessionFactory;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.annotation.Lob;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

/**
 *
 * @author aknahs
 */
public class NdbRMStateStore extends RMStateStore {

    Dispatcher dispatcher;
    private SessionFactory _factory;
    private Session _session;

    public NdbRMStateStore() {
        //Load the properties from the clusterj.properties file
        //TODO: use correct path for the properties file
        File propsFile = new File("src/test/java/org/apache/hadoop/yarn/server/resourcemanager/clusterj.properties");
        InputStream inStream;
        try {
            inStream = new FileInputStream(propsFile);
            Properties props = new Properties();
            props.load(inStream);
            // Create a session (connection to the database)
            _factory = ClusterJHelper.getSessionFactory(props);
            _session = _factory.getSession();
        } catch (FileNotFoundException ex) {
            //TODO : Do better log
            Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public RMState loadState() {
        return new NdbRMState();
    }

    @Override
    protected void initInternal(Configuration conf) throws Exception {
    }

    @Override
    protected void closeInternal() throws Exception {
        
    }

    @Override
    protected void storeApplicationState(String appId, byte[] appStateData) throws Exception {
        //TODO: persist to NDB
    }

    @Override
    protected void storeApplicationAttemptState(String attemptId, byte[] attemptStateData) throws Exception {
        //TODO: persist to NDB
    }

    @Override
    protected void removeApplicationState(String appId) throws Exception {
        //TODO: persist to NDB
    }

    @Override
    protected void removeApplicationAttemptState(String attemptId) throws Exception {
        //TODO: persist to NDB
    }

    public class NdbRMState extends RMState {

        //private HashMap<ApplicationId, ApplicationState> appState = null;
        //new HashMap<ApplicationId, ApplicationState>();

        public NdbRMState() {
            populate();
        }

        @Override
        public Map<ApplicationId, ApplicationState> getApplicationState() {
            populate();
            return appState; 
        }
        
        private void populate()
        {
            appState = new HashMap<ApplicationId, ApplicationState>();
            QueryDomainType<NdbApplicationStateCJ> domainApp;
            QueryDomainType<NdbAttemptStateCJ> domainAttempt;
            Query<NdbApplicationStateCJ> queryApp;
            Query<NdbAttemptStateCJ> queryAttempt;
            List<NdbApplicationStateCJ> resultsApp;
            List<NdbAttemptStateCJ> resultsAttempt;


            //Retrieve applicationstate table
            QueryBuilder builder = _session.getQueryBuilder();
            domainApp = builder.createQueryDefinition(NdbApplicationStateCJ.class);
            domainAttempt = builder.createQueryDefinition(NdbAttemptStateCJ.class);
            queryApp = _session.createQuery(domainApp);
            resultsApp = queryApp.getResultList();

            //Populate appState
            for (NdbApplicationStateCJ storedApp : resultsApp) {
                try {
                    ApplicationId id = new ApplicationIdPBImpl();
                    id.setId(storedApp.getId());
                    id.setClusterTimestamp(storedApp.getClusterTimeStamp());
                    
                    long submitTime = storedApp.getSubmitTime();
                    ApplicationSubmissionContext appSubCon = 
                            new ApplicationSubmissionContextPBImpl(
                            YarnProtos.ApplicationSubmissionContextProto.parseFrom(
                            storedApp.getAppContext()));
                    
                    ApplicationState state = new ApplicationState(submitTime, appSubCon);
                    //state.appId = id;
                    //state.submitTime = storedApp.getSubmitTime();
                    //state.applicationSubmissionContext =
                    //        new ApplicationSubmissionContextPBImpl(
                    //        YarnProtos.ApplicationSubmissionContextProto.parseFrom(
                    //        storedApp.getAppContext()));
                    //state.attempts = new HashMap<ApplicationAttemptId, NdbApplicationAttemptState>();

                    //Populate AppAttempState in each appState
                    //TODO : make sure name is case sensitive
                    domainAttempt.where(domainAttempt.get("applicationId").equal(domainAttempt.param("applicationId")));
                    queryAttempt = _session.createQuery(domainAttempt);
                    queryAttempt.setParameter("applicationId",storedApp.getId());
                    resultsAttempt = queryAttempt.getResultList();
                    
                    for (NdbAttemptStateCJ storedAttempt : resultsAttempt) {
                        ApplicationAttemptId attemptId = new ApplicationAttemptIdPBImpl();
                        attemptId.setApplicationId(id);
                        attemptId.setAttemptId(storedAttempt.getAttemptId());
                        Container masterContainer = new ContainerPBImpl(
                                    YarnProtos.ContainerProto.parseFrom(
                                    storedAttempt.getMasterContainer()));
                        ApplicationAttemptState attemptState = 
                                new ApplicationAttemptState(attemptId, masterContainer);
                        //NdbApplicationAttemptState attemptState = new NdbApplicationAttemptState();
                        //attemptState.masterContainer = new ContainerPBImpl(
                        //        YarnProtos.ContainerProto.parseFrom(
                        //        storedAttempt.getMasterContainer()));
                        state.attempts.put(attemptId, attemptState);
                    }
                    
                    appState.put(id, state);
                } catch (InvalidProtocolBufferException ex) {
                    //TODO : Make a more beatiful exception!
                    Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    @Override
    public void storeApplication(RMApp app) {
        // Create and initialise an NdbApplicationState
        NdbApplicationStateCJ storedApp =
                _session.newInstance(NdbApplicationStateCJ.class);
        storedApp.setId(app.getApplicationId().getId());
        storedApp.setClusterTimeStamp(app.getApplicationId().getClusterTimestamp());
        storedApp.setSubmitTime(app.getSubmitTime());
        byte[] context = ((ApplicationSubmissionContextPBImpl) app.getApplicationSubmissionContext()).getProto().toByteArray();
        storedApp.setAppContext(context);

        //Write NdbApplicationState to ndb database
        _session.persist(storedApp);
    }

    @Override
    public void storeApplicationAttempt(RMAppAttempt appAttempt) {
        NdbAttemptStateCJ storedAttempt =
                _session.newInstance(NdbAttemptStateCJ.class);
        storedAttempt.setAttemptId(appAttempt.getAppAttemptId().getAttemptId());
        storedAttempt.setApplicationId(
                appAttempt.getAppAttemptId().getApplicationId().getId());
        byte[] container = ((ContainerPBImpl) appAttempt.getMasterContainer()).getProto().toByteArray();
        storedAttempt.setMasterContainer(container);

        //Write NdbAttemptState to ndb database
        _session.persist(storedAttempt);
        
        //TODO move this to base class method and remove java imports
        dispatcher.getEventHandler().handle(
        new RMAppAttemptEvent(appAttempt.getAppAttemptId(), 
                              RMAppAttemptEventType.ATTEMPT_SAVED));
    }

    public void clearData()
    {
        _session.deletePersistentAll(NdbApplicationStateCJ.class);
        _session.deletePersistentAll(NdbAttemptStateCJ.class);
    }

    @PersistenceCapable(table = "applicationstate")
    public interface NdbApplicationStateCJ {

        @PrimaryKey
        int getId();
        void setId(int id);

        long getClusterTimeStamp();
        void setClusterTimeStamp(long time);

        long getSubmitTime();
        void setSubmitTime(long time);
	
	@Lob
        byte[] getAppContext();
        void setAppContext(byte[] context);
    }

    @PersistenceCapable(table = "attemptstate")
    public interface NdbAttemptStateCJ {

        @PrimaryKey
        int getAttemptId();
        void setAttemptId(int id);

        @PrimaryKey
        int getApplicationId();
        void setApplicationId(int id);
        
        @Lob  
        byte[] getMasterContainer();
        void setMasterContainer(byte[] state);
    }
}
