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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 *
 * @author aknahs, arinto
 */
public class NdbRMStateStore extends RMStateStore {

    public class NdbRMState extends RMState {

        public NdbRMState() {
            populate();
        }

        @Override
        public Map<ApplicationId, ApplicationState> getApplicationState() {
            populate();
            return appState;
        }

        private void populate() {
            appState = new HashMap<ApplicationId, ApplicationState>();
           
            //Retrieve list of application state
            QueryBuilder builder = _session.getQueryBuilder();
            QueryDomainType<NdbApplicationStateCJ> domainApp = 
                    builder.createQueryDefinition(NdbApplicationStateCJ.class);
            Query<NdbApplicationStateCJ> queryApp = 
                    _session.createQuery(domainApp);
            List<NdbApplicationStateCJ> resultsApp = queryApp.getResultList();
             
            //Populate appState
            for (NdbApplicationStateCJ storedApp : resultsApp) {
                try {
                    ApplicationId id = new ApplicationIdPBImpl();
                    id.setId(storedApp.getId());
                    id.setClusterTimestamp(storedApp.getClusterTimeStamp());

                    ApplicationSubmissionContext appSubContext =
                            new ApplicationSubmissionContextPBImpl(
                            YarnProtos.ApplicationSubmissionContextProto.parseFrom(
                            storedApp.getAppContext()));

                    ApplicationState state = new ApplicationState(
                            storedApp.getSubmitTime(),
                            appSubContext);
                    
                    //prepare local variable for loading attempts
                    QueryDomainType<NdbAttemptStateCJ> domainAttempt =
                            builder.createQueryDefinition(NdbAttemptStateCJ.class);
                    domainAttempt.where(domainAttempt.get("applicationId").equal(
                            domainAttempt.param("applicationId")).and(
                            domainAttempt.get("clusterTimeStamp").equal(
                            domainAttempt.param("clusterTimeStamp"))));

                    Query<NdbAttemptStateCJ> queryAttempt = 
                            _session.createQuery(domainAttempt);
                    queryAttempt.setParameter("applicationId", storedApp.getId());
                    queryAttempt.setParameter("clusterTimeStamp", 
                            storedApp.getClusterTimeStamp());
                    
                    List<NdbAttemptStateCJ> resultsAttempt = 
                            queryAttempt.getResultList();

                    for (NdbAttemptStateCJ storedAttempt : resultsAttempt) {
                        ApplicationAttemptId attemptId = 
                                new ApplicationAttemptIdPBImpl();
                        attemptId.setApplicationId(id);
                        attemptId.setAttemptId(storedAttempt.getAttemptId());
                        
                        Container masterContainer = null;
                        
                        byte[] containerData = storedAttempt.getMasterContainer();
                        if (containerData != null) {
                            masterContainer = new ContainerPBImpl(
                                    YarnProtos.ContainerProto.parseFrom(
                                    containerData));
                        }
                                
                        ApplicationAttemptState attemptState = 
                                new ApplicationAttemptState(
                                attemptId, masterContainer);

                        state.attempts.put(attemptId, attemptState);
                    }

                    appState.put(id, state);
                } catch (InvalidProtocolBufferException ex) {
                    Logger.getLogger(NdbRMStateStore.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    @PersistenceCapable(table = "applicationstate")
    public interface NdbApplicationStateCJ {

        @PrimaryKey
        int getId();
        void setId(int id);

        @PrimaryKey
        long getClusterTimeStamp();
        void setClusterTimeStamp(long time);
        
        long getSubmitTime();
        void setSubmitTime(long time);
        
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
        
        @PrimaryKey
        long getClusterTimeStamp();
        void setClusterTimeStamp(long time);
        
        byte[] getMasterContainer();
        void setMasterContainer(byte[] state);
    }
   
    private SessionFactory _factory;
    private Session _session;
    
    //utility to check whether an instance can be casted into specific type
    private static <T> T as (Class<T> clazz, Object o){
        if(clazz.isInstance(o)){
            return clazz.cast(o);
        }
        return null;
    }
    
    @Override
    public RMState loadState() {
        return new NdbRMState();
    }

    @Override
    protected void initInternal(Configuration conf) throws Exception {
        String clusterJPropFilePath = 
                conf.get(YarnConfiguration.NDB_RM_STATE_STORE_CONFIG_PATH);
        
        if(clusterJPropFilePath == null){
            throw new NullPointerException("Invalid properties file!");
        }
        
        File propsFile = new File(clusterJPropFilePath);
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
    protected void storeApplicationState(String appId, 
    ApplicationStateDataPBImpl appStateData) throws Exception {
    
        ApplicationId applicationId = ConverterUtils.toApplicationId(appId);

        //create Ndb-related instance
        NdbApplicationStateCJ storedApp =
                _session.newInstance(NdbApplicationStateCJ.class);
        
        storedApp.setId(applicationId.getId());
        storedApp.setClusterTimeStamp(applicationId.getClusterTimestamp());
        storedApp.setSubmitTime(appStateData.getSubmitTime());
        ApplicationSubmissionContext appSubContext = 
                appStateData.getApplicationSubmissionContext();
        
        ApplicationSubmissionContextPBImpl appSubContextPbImpl = 
                as(ApplicationSubmissionContextPBImpl.class, appSubContext);
        
        if(appSubContextPbImpl == null){
            throw new NullPointerException("Invalid applicationSubmissionContext data");
        }
        
        storedApp.setAppContext(appSubContextPbImpl.getProto().toByteArray());
        
        _session.persist(storedApp);
               
    }

    @Override
    protected void storeApplicationAttemptState(String attemptId, 
    ApplicationAttemptStateDataPBImpl attemptStateData) throws Exception {
    
        ApplicationAttemptId appAttemptId = 
                ConverterUtils.toApplicationAttemptId(attemptId);
        
        //create Ndb-related instance
        NdbAttemptStateCJ storedAttempt = 
                _session.newInstance(NdbAttemptStateCJ.class);
        
        storedAttempt.setAttemptId(appAttemptId.getAttemptId());
        storedAttempt.setApplicationId(appAttemptId.getApplicationId().getId());
        storedAttempt.setClusterTimeStamp(
                appAttemptId.getApplicationId().getClusterTimestamp());
        
        Container container = attemptStateData.getMasterContainer();
        
        if (container != null) {
            ContainerPBImpl containerPBImpl = as(ContainerPBImpl.class, container);
            
            if (containerPBImpl == null) {
                throw new NullPointerException("Invalid masterContainer data");
            }
            
            storedAttempt.setMasterContainer(containerPBImpl.getProto().toByteArray());
        } else {
            storedAttempt.setMasterContainer(null);
        }
        _session.persist(storedAttempt);
    }

    @Override
    protected void removeApplicationState(ApplicationState appState) throws Exception {
        
        //Construct ApplicationState table's PK
        ApplicationId applicationId = appState.getAppId();
        Integer intAppId = new Integer(applicationId.getId());
        Long longClusterTs = new Long(applicationId.getClusterTimestamp());
        Object[] primaryKey = {intAppId, longClusterTs};
        
        NdbApplicationStateCJ appStateNdb = 
                _session.find(NdbApplicationStateCJ.class, primaryKey);
        
        if(appStateNdb == null){
            //does not need to continue if application state is not valid
            return;
        }
        
        //Delete the attempts first, query the corresponding attempt to be deleted
        //may be it is possible to simplify this by introducing trigger in database (?)
        QueryBuilder builder = _session.getQueryBuilder();
        QueryDomainType<NdbAttemptStateCJ> domainAttempt =
                builder.createQueryDefinition(NdbAttemptStateCJ.class);
        domainAttempt.where(domainAttempt.get("applicationId").equal(
                domainAttempt.param("applicationId")));
        domainAttempt.where(domainAttempt.get("clusterTimeStamp").equal(
                domainAttempt.param("clusterTimeStamp")));

        Query<NdbAttemptStateCJ> queryAttempt = _session.createQuery(domainAttempt);
        queryAttempt.setParameter("applicationId", applicationId.getId());
        queryAttempt.setParameter("clusterTimeStamp", applicationId.getClusterTimestamp());

        List<NdbAttemptStateCJ> resultsAttempt = queryAttempt.getResultList();
        _session.deletePersistentAll(resultsAttempt);
        
        _session.deletePersistent(appStateNdb);
        
    }

    public void clearData()
    {
        _session.deletePersistentAll(NdbApplicationStateCJ.class);
        _session.deletePersistentAll(NdbAttemptStateCJ.class);
    }
}
