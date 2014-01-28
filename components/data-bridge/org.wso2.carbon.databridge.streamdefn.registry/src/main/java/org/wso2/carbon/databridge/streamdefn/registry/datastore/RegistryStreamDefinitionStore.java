/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.carbon.databridge.streamdefn.registry.datastore;

import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.databridge.commons.Credentials;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.commons.utils.DataBridgeCommonsUtils;
import org.wso2.carbon.databridge.commons.utils.EventDefinitionConverterUtils;
import org.wso2.carbon.databridge.core.definitionstore.AbstractStreamDefinitionStore;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.carbon.databridge.streamdefn.registry.internal.ServiceHolder;
import org.wso2.carbon.event.stream.manager.core.EventStreamService;
import org.wso2.carbon.registry.core.RegistryConstants;
import org.wso2.carbon.registry.core.Resource;
import org.wso2.carbon.registry.core.exceptions.RegistryException;
import org.wso2.carbon.registry.core.session.UserRegistry;
import org.wso2.carbon.registry.core.utils.RegistryUtils;
import org.wso2.carbon.user.api.UserStoreException;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The in memory implementation of the Event Stream definition Store
 */
public class RegistryStreamDefinitionStore extends
                                           AbstractStreamDefinitionStore {
    private Log log = LogFactory.getLog(RegistryStreamDefinitionStore.class);

    private static final String STREAM_DEFINITION_STORE = "/StreamDefinitions";


    public StreamDefinition getStreamDefinitionFromStore(Credentials credentials,
                                                         String name, String version)
            throws StreamDefinitionStoreException {

        try {
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext privilegedCarbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();

            privilegedCarbonContext.setTenantId(ServiceHolder.getRealmService().getTenantManager().getTenantId(credentials.getDomainName()));
            privilegedCarbonContext.setTenantDomain(credentials.getDomainName());

            try {
                UserRegistry registry = ServiceHolder.getRegistryService().getGovernanceUserRegistry(credentials.getUsername(), credentials.getPassword());
                if (registry.resourceExists(STREAM_DEFINITION_STORE + RegistryConstants.PATH_SEPARATOR + name + RegistryConstants.PATH_SEPARATOR + version)) {
                    Resource resource = registry.get(STREAM_DEFINITION_STORE + RegistryConstants.PATH_SEPARATOR + name + RegistryConstants.PATH_SEPARATOR + version);
                    Object content = resource.getContent();
                    if (content != null) {
                        return EventDefinitionConverterUtils.convertFromJson(RegistryUtils.decodeBytes((byte[]) resource.getContent()));
                    }
                }
                return null;
            } catch (Exception e) {
                log.error("Error in getting Stream Definition " + name + ":" + version, e);
                throw new StreamDefinitionStoreException("Error in getting Stream Definition " + name + ":" + version, e);
            }

        } catch (UserStoreException e) {
            throw new StreamDefinitionStoreException("Error in getting definition from registry for streamId " + name + ":" + version + ", " + e.getMessage(), e);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }


    }

    @Override
    protected StreamDefinition getStreamDefinitionFromStore(Credentials credentials,
                                                            String streamId)
            throws StreamDefinitionStoreException {

        try {
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext privilegedCarbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();

            privilegedCarbonContext.setTenantId(ServiceHolder.getRealmService().getTenantManager().getTenantId(credentials.getDomainName()));
            privilegedCarbonContext.setTenantDomain(credentials.getDomainName());


            return getStreamDefinitionFromStore(credentials, DataBridgeCommonsUtils.getStreamNameFromStreamId(streamId),
                                                DataBridgeCommonsUtils.getStreamVersionFromStreamId(streamId));

        } catch (UserStoreException e) {
            throw new StreamDefinitionStoreException("Error in getting definition from registry for streamId " + streamId + ", " + e.getMessage(), e);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }

    }

    @Override
    protected boolean removeStreamDefinition(Credentials credentials, String name, String version) {
        try {
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext privilegedCarbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();

            privilegedCarbonContext.setTenantId(ServiceHolder.getRealmService().getTenantManager().getTenantId(credentials.getDomainName()));
            privilegedCarbonContext.setTenantDomain(credentials.getDomainName());


            try {
                UserRegistry registry = ServiceHolder.getRegistryService().getGovernanceUserRegistry(credentials.getUsername(), credentials.getPassword());
                registry.delete(STREAM_DEFINITION_STORE + RegistryConstants.PATH_SEPARATOR + name + RegistryConstants.PATH_SEPARATOR + version);
                return !registry.resourceExists(STREAM_DEFINITION_STORE + RegistryConstants.PATH_SEPARATOR + name + RegistryConstants.PATH_SEPARATOR + version);
            } catch (RegistryException e) {
                log.error("Error in deleting Stream Definition " + name + ":" + version);
            }


        } catch (UserStoreException e) {
            log.error("Error in removing definition from registry for streamId " + name + ":" + version + ", " + e.getMessage(), e);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }

        return false;
    }

    @Override
    protected void saveStreamDefinitionToStore(Credentials credentials,
                                               StreamDefinition streamDefinition)
            throws StreamDefinitionStoreException {
            EventStreamService eventStreamService = ServiceHolder.getEventStreamService();
            AxisConfiguration currentAxisConfiguration = ServiceHolder.getConfigurationContextService().getServerConfigContext().getAxisConfiguration();
            eventStreamService.addStreamDefinitionToStore(credentials,streamDefinition,currentAxisConfiguration);

    }

    public Collection<StreamDefinition> getAllStreamDefinitionsFromStore(
            Credentials credentials) {
        ConcurrentHashMap<String, StreamDefinition> map = new ConcurrentHashMap<String, StreamDefinition>();

        try {
            PrivilegedCarbonContext.startTenantFlow();
            PrivilegedCarbonContext privilegedCarbonContext = PrivilegedCarbonContext.getThreadLocalCarbonContext();
            privilegedCarbonContext.setTenantId(ServiceHolder.getRealmService().getTenantManager().getTenantId(credentials.getDomainName()));
            privilegedCarbonContext.setTenantDomain(credentials.getDomainName());

            try {
                UserRegistry registry = ServiceHolder.getRegistryService().getGovernanceUserRegistry(credentials.getUsername(), credentials.getPassword());

                if (!registry.resourceExists(STREAM_DEFINITION_STORE)) {
                    registry.put(STREAM_DEFINITION_STORE, registry.newCollection());
                } else {
                    org.wso2.carbon.registry.core.Collection collection = (org.wso2.carbon.registry.core.Collection) registry.get(STREAM_DEFINITION_STORE);
                    for (String streamNameCollection : collection.getChildren()) {

                        org.wso2.carbon.registry.core.Collection innerCollection = (org.wso2.carbon.registry.core.Collection) registry.get(streamNameCollection);
                        for (String streamVersionCollection : innerCollection.getChildren()) {

                            Resource resource = (Resource) registry.get(streamVersionCollection);
                            try {
                                StreamDefinition streamDefinition = EventDefinitionConverterUtils.convertFromJson(RegistryUtils.decodeBytes((byte[]) resource.getContent()));
                                map.put(streamDefinition.getStreamId(), streamDefinition);
                            } catch (Throwable e) {
                                log.error("Error in retrieving streamDefinition from the resource at " + resource.getPath(), e);
                            }
                        }
                    }
                }

            } catch (RegistryException e) {
                log.error("Error in retrieving streamDefinitions from the registry", e);
            }

        } catch (UserStoreException e) {
            log.error("Error in getting definitions from registry for user " + credentials.getUsername() + ", " + e.getMessage(), e);
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }

        return map.values();


    }

}
