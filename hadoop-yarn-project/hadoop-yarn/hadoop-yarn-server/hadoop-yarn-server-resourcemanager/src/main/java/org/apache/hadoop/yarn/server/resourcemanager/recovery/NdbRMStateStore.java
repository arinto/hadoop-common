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
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author aknahs
 */
public class NdbRMStateStore extends RMStateStore {

    Dispatcher dispatcher;
    private SessionFactory _factory;
    private Session _session;

    @Override
    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public RMState loadState() {
        //this method is used during recovery, so we need to populate states 
        //from the database!
        //TODO: revisit this method
        return new NdbRMState();
    }

    @Override
    protected void initInternal(Configuration conf) throws Exception {
        //TODO: use conf instance to get the path to clusterj.properties
        File propsFile = new File("src/test/java/org/apache/hadoop/yarn/server/resourcemanager/clusterj.properties");
        InputStream inStream;
        try {
            inStream = new FileInputStream(propsFile);
            Properties props = new Properties();
            props.load(inStream);
            // Create a session(connection to the database)
            _factory = ClusterJHelper.getSessionFactory(props);
            _session = _factory.getSession();
        } catch (FileNotFoundException ex) {
            //rethrow the exception, let the user of this method handle the exception
            Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (IOException ex) {
            //rethrow the execption, let the user of this method handle the exception
            Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
    }

    @Override
    protected void closeInternal() throws Exception {
        //TODO: revisit
    }

    @Override
    protected void storeApplicationState(String appId, byte[] appStateData) throws Exception {  
        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);

        NdbApplicationStateCJ storedApp =
                _session.newInstance(NdbApplicationStateCJ.class);
        storedApp.setId(applicationId.getId());
        storedApp.setClusterTimeStamp(applicationId.getClusterTimestamp());
        storedApp.setAppState(appStateData);
        
        //another option here just to store the submit time and app submission context
        //which is our original design, but we need to deserialize appStateData
        
        _session.persist(storedApp);
    }

    @Override
    protected void storeApplicationAttemptState(String attemptId, byte[] attemptStateData) throws Exception {
        ApplicationAttemptId appAttemptId = 
                ConverterUtils.toApplicationAttemptId(attemptId);
        
        NdbAttemptStateCJ storedAttempt =
                _session.newInstance(NdbAttemptStateCJ.class);
        storedAttempt.setAttemptId(appAttemptId.getAttemptId());
        storedAttempt.setApplicationId(
                appAttemptId.getApplicationId().getId());
        storedAttempt.setClusterTimeStamp(
                appAttemptId.getApplicationId().getClusterTimestamp());
        
        storedAttempt.setAppAttemptState(attemptStateData);
        
        //Write NdbAttemptState to ndb database
        _session.persist(storedAttempt);
    }

    @Override
    protected void removeApplicationState(String appId) throws Exception {
        
        //Query the corresponding attempt to be deleted
        QueryBuilder builder = _session.getQueryBuilder();
        QueryDomainType<NdbAttemptStateCJ> domainAttempt = 
                builder.createQueryDefinition(NdbAttemptStateCJ.class);
        domainAttempt.where(domainAttempt.get("applicationid").equal(
                domainAttempt.param("applicationid")));
        domainAttempt.where(domainAttempt.get("clustertimestamp").equal(
                domainAttempt.param("clustertimestamp")));
        
        Query<NdbAttemptStateCJ> queryAttempt = _session.createQuery(domainAttempt);
        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);
        
        queryAttempt.setParameter("applicationid", applicationId.getId());
        queryAttempt.setParameter("clustertimestamp", applicationId.getClusterTimestamp());
        
        List<NdbAttemptStateCJ> resultsAttempt = queryAttempt.getResultList();
        _session.deletePersistentAll(resultsAttempt);
   
        //Construct ApplicationState table's PK
        Integer intAppId = new Integer(applicationId.getId());
        Long longClusterTs = new Long(applicationId.getClusterTimestamp());
        Object[] primaryKey = {intAppId, longClusterTs}; 
   
        NdbApplicationStateCJ appState = _session.find(NdbApplicationStateCJ.class, 
                primaryKey);
        _session.deletePersistent(NdbApplicationStateCJ.class, appState);

    }

    @Override
    protected void removeApplicationAttemptState(String attemptId) throws Exception {
        ApplicationAttemptId appAttemptId =
                ConverterUtils.toApplicationAttemptId(attemptId);
        
        Integer intAttemptId = new Integer(appAttemptId.getAttemptId());
        Integer intAppId = new Integer(appAttemptId.getApplicationId().getId());
        Long longClusterTs = new Long(appAttemptId.getApplicationId().
                getClusterTimestamp());
        
        Object[] primaryKey = {intAttemptId, intAppId, longClusterTs};
        NdbAttemptStateCJ attempt = 
                _session.find(NdbAttemptStateCJ.class, primaryKey);
        
        if(attempt != null)
        {
            _session.deletePersistent(NdbAttemptStateCJ.class, attempt);
        }     
    }

    public class NdbRMState extends RMState {

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
                    
                    ApplicationStateDataPBImpl appStateData =
                            new ApplicationStateDataPBImpl(
                            YarnProtos.ApplicationStateDataProto.parseFrom(
                            storedApp.getAppState()));
                    
                    ApplicationState state = new ApplicationState(
                            appStateData.getSubmitTime(),
                            appStateData.getApplicationSubmissionContext());

                    domainAttempt.where(domainAttempt.get("applicationid").equal(
                            domainAttempt.param("applicationid")));
                    domainAttempt.where(domainAttempt.get("clustertimestamp").equal(
                            domainAttempt.param("clustertimestamp")));
                    
                    queryAttempt = _session.createQuery(domainAttempt);
                    queryAttempt.setParameter("applicationId",storedApp.getId());
                    queryAttempt.setParameter("clustertimestamp",storedApp.getClusterTimeStamp());
                    resultsAttempt = queryAttempt.getResultList();
                    
                    for (NdbAttemptStateCJ storedAttempt : resultsAttempt) {
                        ApplicationAttemptId attemptId = new ApplicationAttemptIdPBImpl();
                        attemptId.setApplicationId(id);
                        attemptId.setAttemptId(storedAttempt.getAttemptId());
                        
                        ApplicationAttemptStateDataPBImpl attemptStateData =
                                new ApplicationAttemptStateDataPBImpl(
                                YarnProtos.ApplicationAttemptStateDataProto.parseFrom(
                                storedAttempt.getAppAttemptState()));
                        ApplicationAttemptState attemptState = new ApplicationAttemptState(
                                attemptId, attemptStateData.getMasterContainer());
    
                        state.attempts.put(attemptId, attemptState);
                    }
                    
                    appState.put(id, state);
                } catch (InvalidProtocolBufferException ex) {
                    Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
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

        @PrimaryKey
        long getClusterTimeStamp();
        void setClusterTimeStamp(long time);
        
        byte[] getAppState();
        void setAppState(byte[] context);
    }

    @PersistenceCapable(table = "attemptstate")
    public interface NdbAttemptStateCJ {

        @PrimaryKey
        int getAttemptId();
        void setAttemptId(int id);

        @PrimaryKey
        int getApplicationId();
        void setApplicationId(int id);
        
        @PrimaryKey
        long getClusterTimeStamp();
        void setClusterTimeStamp(long time);
        
        byte[] getAppAttemptState();
        void setAppAttemptState(byte[] state);
    }
}
