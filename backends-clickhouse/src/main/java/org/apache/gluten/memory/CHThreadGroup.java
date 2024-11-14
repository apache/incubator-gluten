/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.memory;

import org.apache.spark.TaskContext;
import org.apache.spark.task.TaskResource;
import org.apache.spark.task.TaskResources;

public class CHThreadGroup implements TaskResource {

  /**
   * Register a new thread group for the current task. This method should be called at beginning of
   * the task.
   */
  public static void registerNewThreadGroup() {
    if (TaskResources.isResourceRegistered(CHThreadGroup.class.getName())) return;

    String taskId = "";
    if (TaskResources.getLocalTaskContext() != null) {
      taskId = String.valueOf(TaskResources.getLocalTaskContext().taskAttemptId());
    }

    CHThreadGroup group = new CHThreadGroup(taskId);
    TaskResources.addResource(CHThreadGroup.class.getName(), group);
    TaskContext.get()
        .addTaskCompletionListener(
            (context -> {
              context.taskMetrics().incPeakExecutionMemory(group.getPeakMemory());
            }));
  }

  private long thread_group_id = 0;
  private long peak_memory = -1;

  private CHThreadGroup(String taskId) {
    thread_group_id = createThreadGroup(taskId);
  }

  public long getPeakMemory() {
    if (peak_memory < 0) {
      peak_memory = threadGroupPeakMemory(thread_group_id);
    }
    return peak_memory;
  }

  @Override
  public void release() throws Exception {
    if (peak_memory < 0) {
      peak_memory = threadGroupPeakMemory(thread_group_id);
    }
    releaseThreadGroup(thread_group_id);
  }

  @Override
  public int priority() {
    return TaskResource.super.priority();
  }

  @Override
  public String resourceName() {
    return "CHThreadGroup";
  }

  private static native long createThreadGroup(String taskId);

  private static native long threadGroupPeakMemory(long id);

  private static native void releaseThreadGroup(long id);
}
