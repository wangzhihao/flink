/*
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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The {@link DummyEnvironment} is used for test purpose. */
public class DummyEnvironment implements Environment {

    private final JobInfo jobInfo = new JobInfoImpl(new JobID(), "DummyJob");
    private final JobVertexID jobVertexId = new JobVertexID();
    private final JobType jobType = JobType.STREAMING;
    private final ExecutionAttemptID executionId;
    private final ExecutionConfig executionConfig = new ExecutionConfig();
    private final TaskInfo taskInfo;
    private KvStateRegistry kvStateRegistry = new KvStateRegistry();
    private TaskStateManager taskStateManager;
    private final GlobalAggregateManager aggregateManager;
    private final AccumulatorRegistry accumulatorRegistry;
    private UserCodeClassLoader userClassLoader;
    private final Configuration taskConfiguration = new Configuration();
    private final ChannelStateWriteRequestExecutorFactory channelStateExecutorFactory =
            new ChannelStateWriteRequestExecutorFactory(jobInfo.getJobId());

    private CheckpointStorageAccess checkpointStorageAccess;

    public DummyEnvironment() {
        this("Test Job", 1, 0, 1);
    }

    public DummyEnvironment(ClassLoader userClassLoader) {
        this("Test Job", 1, 0, 1);
        this.userClassLoader =
                TestingUserCodeClassLoader.newBuilder().setClassLoader(userClassLoader).build();
    }

    public DummyEnvironment(String taskName, int numSubTasks, int subTaskIndex) {
        this(taskName, numSubTasks, subTaskIndex, numSubTasks);
    }

    public DummyEnvironment(
            String taskName, int numSubTasks, int subTaskIndex, int maxParallelism) {
        this.taskInfo = new TaskInfoImpl(taskName, maxParallelism, subTaskIndex, numSubTasks, 0);
        this.executionId =
                createExecutionAttemptId(jobVertexId, subTaskIndex, taskInfo.getAttemptNumber());
        this.taskStateManager = new TestTaskStateManager();
        this.aggregateManager = new TestGlobalAggregateManager();
        this.accumulatorRegistry = new AccumulatorRegistry(jobInfo.getJobId(), executionId);
    }

    public void setKvStateRegistry(KvStateRegistry kvStateRegistry) {
        this.kvStateRegistry = kvStateRegistry;
    }

    public KvStateRegistry getKvStateRegistry() {
        return kvStateRegistry;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public JobID getJobID() {
        return jobInfo.getJobId();
    }

    @Override
    public JobType getJobType() {
        return jobType;
    }

    @Override
    public JobVertexID getJobVertexId() {
        return jobVertexId;
    }

    @Override
    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    @Override
    public Configuration getTaskConfiguration() {
        return taskConfiguration;
    }

    @Override
    public TaskManagerRuntimeInfo getTaskManagerInfo() {
        return new TestingTaskManagerRuntimeInfo();
    }

    @Override
    public TaskMetricGroup getMetricGroup() {
        return UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
    }

    @Override
    public Configuration getJobConfiguration() {
        return new Configuration();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public InputSplitProvider getInputSplitProvider() {
        return null;
    }

    @Override
    public IOManager getIOManager() {
        return null;
    }

    @Override
    public MemoryManager getMemoryManager() {
        return null;
    }

    @Override
    public SharedResources getSharedResources() {
        return null;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        if (userClassLoader == null) {
            return TestingUserCodeClassLoader.newBuilder().build();
        } else {
            return userClassLoader;
        }
    }

    @Override
    public Map<String, Future<Path>> getDistributedCacheEntries() {
        return Collections.emptyMap();
    }

    @Override
    public BroadcastVariableManager getBroadcastVariableManager() {
        return null;
    }

    @Override
    public TaskStateManager getTaskStateManager() {
        return taskStateManager;
    }

    @Override
    public GlobalAggregateManager getGlobalAggregateManager() {
        return aggregateManager;
    }

    @Override
    public AccumulatorRegistry getAccumulatorRegistry() {
        return accumulatorRegistry;
    }

    @Override
    public TaskKvStateRegistry getTaskKvStateRegistry() {
        return kvStateRegistry.createTaskRegistry(jobInfo.getJobId(), jobVertexId);
    }

    @Override
    public void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics) {}

    @Override
    public ExternalResourceInfoProvider getExternalResourceInfoProvider() {
        return ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;
    }

    @Override
    public void acknowledgeCheckpoint(
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState) {}

    @Override
    public void declineCheckpoint(long checkpointId, CheckpointException cause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failExternally(Throwable cause) {
        throw new UnsupportedOperationException(
                "DummyEnvironment does not support external task failure.");
    }

    @Override
    public ResultPartitionWriter getWriter(int index) {
        return null;
    }

    @Override
    public ResultPartitionWriter[] getAllWriters() {
        return new ResultPartitionWriter[0];
    }

    @Override
    public IndexedInputGate getInputGate(int index) {
        throw new ArrayIndexOutOfBoundsException(0);
    }

    @Override
    public IndexedInputGate[] getAllInputGates() {
        return new IndexedInputGate[0];
    }

    @Override
    public TaskEventDispatcher getTaskEventDispatcher() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskManagerActions getTaskManagerActions() {
        throw new UnsupportedOperationException();
    }

    public void setTaskStateManager(TaskStateManager taskStateManager) {
        this.taskStateManager = taskStateManager;
    }

    @Override
    public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
        return new NoOpTaskOperatorEventGateway();
    }

    @Override
    public ChannelStateWriteRequestExecutorFactory getChannelStateExecutorFactory() {
        return channelStateExecutorFactory;
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    @Override
    public void setCheckpointStorageAccess(CheckpointStorageAccess checkpointStorageAccess) {
        this.checkpointStorageAccess = checkpointStorageAccess;
    }

    @Override
    public CheckpointStorageAccess getCheckpointStorageAccess() {
        return checkNotNull(checkpointStorageAccess);
    }
}
