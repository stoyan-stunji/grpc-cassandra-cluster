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
package org.apache.cassandra.exceptions;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Exception codes for internal CassandraException serialization.
 * Each concrete CassandraException subclass has a unique code to ensure proper type fidelity during serialization.
 * This is separate from the client-facing ExceptionCode enum which maintains backward compatibility.
 */
@Shared(scope = SIMULATION)
public enum CassandraExceptionCode
{
    UNAVAILABLE                                 (0x1000),  // UnavailableException
    FUNCTION_FAILURE                            (0x1001),  // FunctionExecutionException
    OPERATION_EXECUTION                         (0x1002),  // OperationExecutionException
    OVERLOADED                                  (0x1003),  // OverloadedException
    CAS_WRITE_UNKNOWN                           (0x1004),  // CasWriteUnknownResultException
    IS_BOOTSTRAPPING                            (0x1005),  // IsBootstrappingException
    CDC_WRITE_FAILURE                           (0x1006),  // CDCWriteException
    TRUNCATE_ERROR                              (0x1007),  // TruncateException
    READ_TIMEOUT                                (0x1200),  // ReadTimeoutException
    ACCORD_READ_EXHAUSTED                       (0x1201),  // AccordReadExhaustedException
    ACCORD_READ_PREEMPTED                       (0x1202),  // AccordReadPreemptedException
    WRITE_TIMEOUT                               (0x1210),  // WriteTimeoutException
    ACCORD_WRITE_PREEMPTED                      (0x1211),  // AccordWritePreemptedException
    ACCORD_WRITE_EXHAUSTED                      (0x1212),  // AccordWriteExhaustedException
    CAS_WRITE_TIMEOUT                           (0x1213),  // CasWriteTimeoutException
    READ_FAILURE                                (0x1300),  // ReadFailureException
    TOMBSTONE_ABORT                             (0x1301),  // TombstoneAbortException
    READ_SIZE_ABORT                             (0x1302),  // ReadSizeAbortException
    QUERY_TOO_MANY_INDEXES_ABORT                (0x1303),  // QueryReferencesTooManyIndexesAbortException
    WRITE_FAILURE                               (0x1310),  // WriteFailureException
    UNPREPARED                                  (0x2000),  // PreparedQueryNotFoundException
    BAD_CREDENTIALS                             (0x2001),  // AuthenticationException
    CONFIG_ERROR                                (0x2002),  // ConfigurationException
    ALREADY_EXISTS                              (0x2003),  // AlreadyExistsException
    UNAUTHORIZED                                (0x2004),  // UnauthorizedException
    INVALID                                     (0x2005),  // InvalidRequestException
    INVALID_CONSTRAINT_DEFINITION               (0x2006),  // InvalidConstraintDefinitionException
    CONSTRAINT_VIOLATION                        (0x2007),  // ConstraintViolationException
    OVERSIZED_MESSAGE                           (0x2008),  // OversizedCQLMessageException
    TRIGGER_DISABLED                            (0x2009),  // TriggerDisabledException
    KEYSPACE_NOT_DEFINED                        (0x200A),  // KeyspaceNotDefinedException
    GUARDRAIL_VIOLATED                          (0x200B),  // GuardrailViolatedException
    INVALID_ROUTING                             (0x200C),  // InvalidRoutingException
    MUTATION_EXCEEDED_MAX_SIZE                  (0x200D),  // MutationExceededMaxSizeException
    SYNTAX_ERROR                                (0x200E);  // SyntaxException

    public final int value;
    private static final Map<Integer, CassandraExceptionCode> valueToCode = new HashMap<>(CassandraExceptionCode.values().length);

    static
    {
        for (CassandraExceptionCode code : CassandraExceptionCode.values())
            valueToCode.put(code.value, code);
    }

    CassandraExceptionCode(int value)
    {
        this.value = value;
    }

    public static CassandraExceptionCode fromValue(int value)
    {
        CassandraExceptionCode code = valueToCode.get(value);
        if (code == null)
            throw new IllegalArgumentException(String.format("Unknown CassandraException code %d", value));
        return code;
    }
}