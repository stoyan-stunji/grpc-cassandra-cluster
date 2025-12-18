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

package org.apache.cassandra.db.guardrails;

import org.apache.cassandra.exceptions.CassandraExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class GuardrailViolatedException extends InvalidRequestException
{
    public GuardrailViolatedException(String message)
    {
        super(message);
    }

    protected GuardrailViolatedException(String message, Throwable cause)
    {
        super(message, cause);
    }

    @Override
    public CassandraExceptionCode getCassandraExceptionCode()
    {
        return CassandraExceptionCode.GUARDRAIL_VIOLATED;
    }

    /**
     * Wraps a guardrail exception for deferred throwing, preserving the exception type.
     * This is used when a guardrail exception is caught during CAS request preparation
     * but needs to be thrown later after conditions are checked.
     *
     * @param original the original exception that was deferred
     * @return a new exception of the same type with the original as the cause
     */
    public static GuardrailViolatedException wrapForDeferredThrow(GuardrailViolatedException original)
    {
        if (original instanceof PasswordGuardrail.PasswordGuardrailException)
        {
            PasswordGuardrail.PasswordGuardrailException pge = (PasswordGuardrail.PasswordGuardrailException) original;
            return new PasswordGuardrail.PasswordGuardrailException(pge.getMessage(), pge.redactedMessage, original);
        }
        return new GuardrailViolatedException(original.getMessage(), original);
    }
}
