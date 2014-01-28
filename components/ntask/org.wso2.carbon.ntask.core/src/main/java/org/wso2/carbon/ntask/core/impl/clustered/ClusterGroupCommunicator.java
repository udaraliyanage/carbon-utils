/**
 *  Copyright (c) 2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.carbon.ntask.core.impl.clustered;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.common.TaskException.Code;
import org.wso2.carbon.ntask.core.TaskManager;
import org.wso2.carbon.ntask.core.impl.clustered.rpc.TaskCall;
import org.wso2.carbon.ntask.core.internal.TasksDSComponent;
import org.wso2.carbon.ntask.core.service.TaskService;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * This class represents the cluster group communicator used by clustered task
 * managers.
 */
public class ClusterGroupCommunicator implements MembershipListener {

    private static final String NTASK_P2P_COMM_EXECUTOR = "__NTASK_P2P_COMM_EXECUTOR__";

    private static final String TASK_SERVER_STARTUP_COUNTER = "__TASK_SERVER_STARTUP_COUNTER__";

    private static final int MISSING_TASKS_ON_ERROR_RETRY_COUNT = 3;

    private static final String CARBON_TASKS_MEMBER_ID_QUEUE = "__CARBON_TASKS_MEMBER_ID_QUEUE__";

    public static final String TASK_SERVER_COUNT_SYS_PROP = "task.server.count";

    private static final Log log = LogFactory.getLog(ClusterGroupCommunicator.class);

    private static ClusterGroupCommunicator instance;

    private TaskService taskService;

    private HazelcastInstance hazelcast;

    private Map<String, Member> membersMap = new ConcurrentHashMap<String, Member>();

    private Queue<String> membersQueue;

    public static ClusterGroupCommunicator getInstance() throws TaskException {
        if (instance == null) {
            synchronized (ClusterGroupCommunicator.class) {
                if (instance == null) {
                    instance = new ClusterGroupCommunicator();
                }
            }
        }
        return instance;
    }

    private ClusterGroupCommunicator() throws TaskException {
        this.taskService = TasksDSComponent.getTaskService();
        this.hazelcast = TasksDSComponent.getHazelcastInstance();
        if (this.getHazelcast() == null) {
            throw new TaskException("ClusterGroupCommunicator cannot initialize, " +
            		"Hazelcast is not initialized", Code.CONFIG_ERROR);
        }
        this.getHazelcast().getCluster().addMembershipListener(this);
        for (Member member : this.getHazelcast().getCluster().getMembers()) {
            this.membersMap.put(this.getIdFromMember(member), member);
        }
        /* create a distributed queue to track the leader */
        this.membersQueue = this.getHazelcast().getQueue(CARBON_TASKS_MEMBER_ID_QUEUE);
        this.membersQueue.add(this.getMemberId());
        /* increment the task server count */
        this.getHazelcast().getAtomicLong(TASK_SERVER_STARTUP_COUNTER).incrementAndGet();
    }

    private String getIdFromMember(Member member) {
        return member.getUuid();
    }

    private Member getMemberFromId(String id) {
        return membersMap.get(id);
    }

    public void checkServers() throws TaskException {
        int serverCount = this.getTaskService().getServerConfiguration().getTaskServerCount();
        if (serverCount != -1) {
            log.info("Waiting for " + serverCount + " task executor nodes...");
            try {
                /* with this approach, lets say the server count is 3, and after all 3 server comes up, 
                 * and tasks scheduled, if two nodes go away, and one comes up, it will be allowed to start,
                 * even though there aren't 3 live nodes, which would be the correct approach, if the whole
                 * cluster goes down, then, you need again for all 3 of them to come up */
                while (this.getHazelcast().getAtomicLong(TASK_SERVER_STARTUP_COUNTER).get() < serverCount) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                throw new TaskException("Error in waiting for task executor nodes: " + e.getMessage(), 
                        Code.UNKNOWN, e);
            }
            log.info("All task servers activated.");
        }
    }

    public TaskService getTaskService() {
        return taskService;
    }

    public HazelcastInstance getHazelcast() {
        return hazelcast;
    }

    public String getLeaderId() throws TaskException {
        return null;
    }

    public synchronized List<String> getMemberIds() throws TaskException {
        /* refresh the member id map here also, will be needed for failure situations
         * where multiple servers go down once, and when a notification for only a single
         * member departure comes, but actually when we get all the members, many may have left */
        this.membersMap.clear();
        for (Member member : this.getHazelcast().getCluster().getMembers()) {
            this.membersMap.put(this.getIdFromMember(member), member);
        }
        return new ArrayList<String>(this.membersMap.keySet());
    }

    public String getMemberId() {
        return this.getIdFromMember(this.getHazelcast().getCluster().getLocalMember());
    }

    public boolean isLeader() {
        if (this.getHazelcast().getLifecycleService().isRunning()) {
            return this.getMemberId().equals(this.membersQueue.peek());
        }
        return false;
    }

    public <V> V sendReceive(String memberId, TaskCall<V> taskCall) throws TaskException {
        IExecutorService es = this.getHazelcast().getExecutorService(NTASK_P2P_COMM_EXECUTOR);
        Future<V> taskExec = es.submitToMember(taskCall, this.getMemberFromId(memberId));
        try {
            return taskExec.get();
        } catch (Exception e) {
            throw new TaskException("Error in cluster message send-receive: " + e.getMessage(),
                    Code.UNKNOWN, e);
        }
    }

    @Override
    public void memberAdded(MembershipEvent event) {
        if (this.getHazelcast().getLifecycleService().isRunning()) {
            Member member = event.getMember();
            this.membersMap.put(this.getIdFromMember(member), member);
        }
    }

    private void scheduleAllMissingTasks() throws TaskException {
        for (String taskType : this.getTaskService().getRegisteredTaskTypes()) {
            for (TaskManager tm : getTaskService().getAllTenantTaskManagersForType(taskType)) {
                if (tm instanceof ClusteredTaskManager) {
                    this.scheduleMissingTasksWithRetryOnError((ClusteredTaskManager) tm);
                }
            }
        }
    }

    private void scheduleMissingTasksWithRetryOnError(ClusteredTaskManager tm) {
        int count = MISSING_TASKS_ON_ERROR_RETRY_COUNT;
        while (count > 0) {
            try {
                tm.scheduleMissingTasks();
                break;
            } catch (TaskException e) {
                log.error("Encountered error(s) in scheduling missing tasks ["
                        + tm.getTaskType() + "][" + tm.getTenantId() + "]:- \n" +
                        e.getMessage() + "\n" + ((count > 1) ? "Retrying [" +
                        ((MISSING_TASKS_ON_ERROR_RETRY_COUNT - count) + 1) + "]..." : "Giving up."));
            }
            count--;
        }
    }

    @Override
    public void memberRemoved(MembershipEvent event) {
        if (this.getHazelcast().getLifecycleService().isRunning()) {
            String id = this.getIdFromMember(event.getMember());
            this.membersMap.remove(id);
            this.membersQueue.remove(id);
            try {
                if (this.isLeader()) {
                    log.info("Task member departed [" + event.getMember().toString()
                            + "], rescheduling missing tasks...");
                    this.scheduleAllMissingTasks();
                }
            } catch (TaskException e) {
                log.error("Error in scheduling missing tasks: " + e.getMessage(), e);
            }
        }
    }

}
