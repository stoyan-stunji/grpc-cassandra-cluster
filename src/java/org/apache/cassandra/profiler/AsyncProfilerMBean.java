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

package org.apache.cassandra.profiler;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerEvent;
import org.apache.cassandra.service.AsyncProfilerService.AsyncProfilerFormat;

public interface AsyncProfilerMBean
{
    String MBEAN_NAME = "org.apache.cassandra.profiler:type=AsyncProfiler";

    /**
     * Starts profiling.
     *
     * @param events         events, can be joined by a comma, each event has to be one of enum names of
     *                       {@link AsyncProfilerEvent}
     * @param outputFormat   output format, has to be one of enum names of {@link AsyncProfilerFormat}
     * @param duration       duration of the profiling, Accepts string values in the
     *                       form of {@code '5m'}, {@code '30s'} etc.
     * @param outputFileName file name to save results to
     * @return true if profiling has started, false when not (e.g. when it was started already)
     */
    boolean start(String events, String outputFormat, String duration, String outputFileName);

    /**
     * Stops profiling.
     *
     * @param outputFileName file name to save results to
     * @return true if profiling was stopped
     */
    boolean stop(String outputFileName);

    /**
     * Executes a command.
     *
     * @param command command to execute.
     * @return execution result
     */
    String execute(String command);

    /**
     * Checks if a profiler is available.
     *
     * @return true if async profiling is enabled and profiler is initialized, false otherwise.
     */
    boolean isEnabled();

    /**
     * Removes all profile files from disk.
     */
    void purge();

    /**
     * Returns list of files where profiler was saving results.
     *
     * @return list of profiler result files
     */
    List<String> list();

    /**
     * Returns the content of a result file. Use {@link #list()} to get their names.
     *
     * @param resultFile file with profiler results
     * @return content of profiler resuls file, or null, when not found.
     * @throws IOException not found or other exception occurred
     */
    byte[] fetch(String resultFile) throws IOException;

    /**
     * @return status the profiler is in, as string description, for diagnostic purposes
     */
    String status();
}
