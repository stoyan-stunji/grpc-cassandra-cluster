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
package org.apache.cassandra.service;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.utils.FBUtilities;

public class ClientWarn extends ExecutorLocals.Impl
{
    private static final String TRUNCATED = " [truncated]";
    public static ClientWarn instance = new ClientWarn();

    private ClientWarn()
    {
    }

    public State get()
    {
        return ExecutorLocals.current().clientWarnState;
    }

    public void set(State value)
    {
        ExecutorLocals current = ExecutorLocals.current();
        ExecutorLocals.Impl.set(current.traceState, value);
    }

    public void warn(String text)
    {
        State state = get();
        if (state != null)
            state.add(text);
    }

    public void captureWarnings()
    {
        set(new State());
    }

    /**
     * Provides an additional control on capturing warnings. When executing SchemaTransformations in the
     * metadata log follower or when committing on a CMS member, we don't want these to be triggered.
     * @see org.apache.cassandra.schema.SchemaTransformation#enterExecution()
     **/
    public void pauseCapture()
    {
        State state = get();
        if (state != null)
            state.collecting = false;
    }

    public void resumeCapture()
    {
        State state = get();
        if (state != null)
            state.collecting = true;
    }

    public List<String> getWarnings()
    {
        State state = get();
        if (state == null || state.warnings == null || state.warnings.isEmpty())
            return null;
        return state.warnings;
    }

    public void resetWarnings()
    {
        set(null);
    }

    /**
     * Start deferring warnings. Any warnings added after this call will be stored
     * separately and can later be committed (inserted at the marked position) or discarded.
     * The insertion point is recorded as the current size of the warnings list.
     * <p>
     * This is used for CAS operations where we don't want to emit warnings until
     * we know whether the conditions passed.
     */
    public void startDeferring()
    {
        State state = get();
        if (state != null)
            state.startDeferring();
    }

    /**
     * Check if we're currently deferring warnings.
     */
    public boolean isDeferring()
    {
        State state = get();
        return state != null && state.isDeferring();
    }

    /**
     * Commit deferred warnings by inserting them at the recorded insertion point.
     * After this call, deferring mode is disabled.
     *
     * @param replayAction optional consumer to receive deferred actions for replay (e.g., guardrail diagnostic events)
     */
    public void commitDeferredWarnings(Consumer<Runnable> replayAction)
    {
        State state = get();
        if (state != null)
            state.commitDeferredWarnings(replayAction);
    }

    /**
     * Commit deferred warnings by inserting them at the recorded insertion point.
     * After this call, deferring mode is disabled.
     */
    public void commitDeferredWarnings()
    {
        commitDeferredWarnings(Runnable::run);
    }

    /**
     * Discard all deferred warnings without adding them to the main warnings list.
     * After this call, deferring mode is disabled.
     */
    public void discardDeferredWarnings()
    {
        State state = get();
        if (state != null)
            state.discardDeferredWarnings();
    }

    /**
     * Store a deferred action to be executed when warnings are committed.
     * This is used to defer guardrail diagnostic events until we know conditions passed.
     */
    public void addDeferredAction(Runnable action)
    {
        State state = get();
        if (state != null)
            state.addDeferredAction(action);
    }

    public static class State
    {
        private boolean collecting = true;
        // This must be a thread-safe list. Even though it's wrapped in a ThreadLocal, it's propagated to each thread
        // from shared state, so multiple threads can reference the same State.
        private volatile List<String> warnings;

        // Deferred warnings support for CAS operations
        // These must also be thread-safe since State can be shared across threads
        private volatile List<String> deferredWarnings;
        private volatile List<Runnable> deferredActions;
        private volatile int insertionPoint = -1;

        private void add(String warning)
        {
            if (warnings == null)
                synchronized (this)
                {
                    if (warnings == null)
                    {
                        warnings = new CopyOnWriteArrayList<>();
                    }
                }

            if (!collecting)
                return;

            if (warnings.size() >= FBUtilities.MAX_UNSIGNED_SHORT)
                return;

            String truncatedWarning = maybeTruncate(warning);

            // If deferring, add to deferred list instead
            if (insertionPoint >= 0)
            {
                if (deferredWarnings == null)
                    synchronized (this)
                    {
                        if (deferredWarnings == null)
                            deferredWarnings = new CopyOnWriteArrayList<>();
                    }
                deferredWarnings.add(truncatedWarning);
            }
            else
            {
                warnings.add(truncatedWarning);
            }
        }

        private static String maybeTruncate(String warning)
        {
            return warning.length() > FBUtilities.MAX_UNSIGNED_SHORT
                   ? warning.substring(0, FBUtilities.MAX_UNSIGNED_SHORT - TRUNCATED.length()) + TRUNCATED
                   : warning;
        }

        /**
         * Start deferring warnings. Records the current position in the warnings list
         * as the insertion point for when deferred warnings are committed.
         */
        synchronized void startDeferring()
        {
            insertionPoint = warnings == null ? 0 : warnings.size();
            deferredWarnings = null;
            deferredActions = null;
        }

        /**
         * Check if currently deferring warnings.
         */
        boolean isDeferring()
        {
            return insertionPoint >= 0;
        }

        /**
         * Add a deferred action to be executed when warnings are committed.
         */
        void addDeferredAction(Runnable action)
        {
            if (insertionPoint < 0)
            {
                // Not deferring, execute immediately
                action.run();
                return;
            }
            if (deferredActions == null)
                synchronized (this)
                {
                    if (deferredActions == null)
                        deferredActions = new CopyOnWriteArrayList<>();
                }
            deferredActions.add(action);
        }

        /**
         * Commit deferred warnings by inserting them at the recorded insertion point.
         */
        void commitDeferredWarnings(Consumer<Runnable> replayAction)
        {
            if (insertionPoint < 0)
                return;

            if (deferredWarnings != null && !deferredWarnings.isEmpty())
            {
                // Ensure warnings list exists
                if (warnings == null)
                    synchronized (this)
                    {
                        if (warnings == null)
                        {
                            warnings = new CopyOnWriteArrayList<>();
                        }
                    }

                // Insert deferred warnings at the insertion point
                // Note: CopyOnWriteArrayList.addAll(int, Collection) is atomic
                int availableSpace = FBUtilities.MAX_UNSIGNED_SHORT - warnings.size();
                if (availableSpace > 0)
                {
                    List<String> toInsert = deferredWarnings.size() <= availableSpace
                                            ? deferredWarnings
                                            : deferredWarnings.subList(0, availableSpace);
                    warnings.addAll(Math.min(insertionPoint, warnings.size()), toInsert);
                }
            }

            // Execute deferred actions
            if (deferredActions != null)
            {
                for (Runnable action : deferredActions)
                    replayAction.accept(action);
            }

            insertionPoint = -1;
            deferredWarnings = null;
            deferredActions = null;
        }

        /**
         * Discard deferred warnings without committing them.
         */
        void discardDeferredWarnings()
        {
            insertionPoint = -1;
            deferredWarnings = null;
            deferredActions = null;
        }
    }
}
