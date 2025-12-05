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
package org.apache.cassandra.service.paxos;

import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.RemoteClientState;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CasForwardingTest
{
    private static final String KEYSPACE1 = "CasForwardingTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testCasForwardResponseExceptionHandling() throws Exception
    {
        // Test exception forwarding in CasForwardResponse
        UnavailableException testException = new UnavailableException("Test exception", ConsistencyLevel.QUORUM, 3, 1);

        CasForwardResponse response = new CasForwardResponse(testException, null);

        assertFalse("Response should not be successful", response.isSuccess());
        assertEquals("Exception should match", testException, response.exception);
        assertNull("Result should be null when exception is present", response.result);
        assertNull("Warnings should be null", response.warnings);
    }

    @Test
    public void testCasForwardResponseSuccess() throws Exception
    {
        // Test successful response - explicitly cast null to RowIterator to resolve constructor ambiguity
        CasForwardResponse response = new CasForwardResponse((org.apache.cassandra.db.rows.RowIterator) null, null);

        assertTrue("Response should be successful", response.isSuccess());
        assertNull("Exception should be null for success", response.exception);
    }

    @Test
    public void testCasForwardRequestWithRemoteClientState() throws Exception
    {
        // Test CasForwardRequest with RemoteClientState serialization
        // Note: This test only verifies the CasForwardRequest can be constructed with RemoteClientState.
        // Serialization roundtrip requires a real CQL3CasRequest which is complex to construct.
        DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes("test"));
        ClientState localState = ClientState.forInternalCalls();
        localState.setKeyspace(KEYSPACE1);

        CasForwardRequest request = new CasForwardRequest(
            KEYSPACE1,
            CF_STANDARD1,
            key,
            null, // CASRequest - simplified for test
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.QUORUM,
            localState,
            System.currentTimeMillis()
        );

        // Verify RemoteClientState was created correctly
        assertNotNull("Client state should not be null", request.clientState);
        assertTrue("Client state should be RemoteClientState", request.clientState instanceof RemoteClientState);
        assertEquals("Keyspace should match", KEYSPACE1, request.clientState.getRawKeyspace());

        // Verify other fields
        assertEquals("Keyspace name should match", KEYSPACE1, request.keyspaceName);
        assertEquals("CF name should match", CF_STANDARD1, request.cfName);
        assertEquals("Consistency for paxos should match", ConsistencyLevel.QUORUM, request.consistencyForPaxos);
        assertEquals("Consistency for commit should match", ConsistencyLevel.QUORUM, request.consistencyForCommit);
    }

    // Note: Full serialization testing for CasForwardRequest requires a real CQL3CasRequest,
    // which is complex to construct in unit tests. The serialization infrastructure is tested
    // through integration tests. This test verifies the non-serialization aspects of the request.

    @Test
    public void testCasForwardResponseSerialization() throws Exception
    {
        // Test exception serialization
        UnavailableException testException = new UnavailableException("Test serialization exception", ConsistencyLevel.QUORUM, 3, 1);
        CasForwardResponse originalResponse = new CasForwardResponse(testException, null);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        CasForwardResponse.serializer.serialize(originalResponse, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        CasForwardResponse deserializedResponse = CasForwardResponse.serializer.deserialize(in, 0);

        // Verify exception is preserved
        assertFalse("Response should not be successful", deserializedResponse.isSuccess());
        assertNotNull("Exception should not be null", deserializedResponse.exception);
        assertEquals("Exception type should match", testException.getClass(), deserializedResponse.exception.getClass());

        // Note: UnavailableException reconstructs its message from consistency level, required, and alive values,
        // so we verify the exception type and key properties rather than the exact message
        UnavailableException deserializedException = (UnavailableException) deserializedResponse.exception;
        assertEquals("Consistency level should match", testException.consistency, deserializedException.consistency);
        assertEquals("Required replicas should match", testException.required, deserializedException.required);
        assertEquals("Alive replicas should match", testException.alive, deserializedException.alive);
    }

    @Test
    public void testCasForwardResponseSerializedSizeAccuracy() throws Exception
    {
        // Test that serializedSize method returns accurate sizes
        UnavailableException testException = new UnavailableException("Size test exception", ConsistencyLevel.QUORUM, 3, 1);
        CasForwardResponse response = new CasForwardResponse(testException, null);

        // Calculate expected size
        long calculatedSize = CasForwardResponse.serializer.serializedSize(response, 0);

        // Serialize and measure actual size
        DataOutputBuffer out = new DataOutputBuffer();
        CasForwardResponse.serializer.serialize(response, out, 0);

        long actualSize = out.getLength();

        assertEquals("Calculated size should match actual serialized size", calculatedSize, actualSize);
    }

    @Test
    public void testCasForwardResponseNullResultSerialization() throws Exception
    {
        // Test serialization of successful response with null result
        CasForwardResponse originalResponse = new CasForwardResponse((RowIterator) null, null);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        CasForwardResponse.serializer.serialize(originalResponse, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        CasForwardResponse deserializedResponse = CasForwardResponse.serializer.deserialize(in, 0);

        // Verify success and null result
        assertTrue("Response should be successful", deserializedResponse.isSuccess());
        assertNull("Exception should be null", deserializedResponse.exception);
        assertNull("Result should be null", deserializedResponse.result);
    }

    @Test
    public void testConsensusReadForwardRequestCreation() throws Exception
    {
        // Test basic creation of ConsensusReadForwardRequest
        // ConsensusReadForwardRequest now takes a single command, so we just test with null for simplicity
        ConsensusReadForwardRequest request = new ConsensusReadForwardRequest(
            null,
            ConsistencyLevel.QUORUM
        );

        assertNotNull("ConsensusReadForwardRequest should be created", request);
        assertNull("Command should be null for this test", request.command);
        assertEquals("Consistency level should match", ConsistencyLevel.QUORUM, request.consistencyLevel);
    }

    @Test
    public void testConsensusReadForwardResponseExceptionHandling() throws Exception
    {
        // Test exception forwarding in ConsensusReadForwardResponse
        UnavailableException testException = new UnavailableException("Test exception", ConsistencyLevel.QUORUM, 3, 1);

        ConsensusReadForwardResponse response = new ConsensusReadForwardResponse(testException, null);

        assertFalse("Response should not be successful", response.isSuccess());
        assertEquals("Exception should match", testException, response.exception);
        assertNull("Result should be null when exception is present", response.getResult());
        assertNull("Warnings should be null", response.warnings);
    }

    @Test
    public void testCasForwardResponseWarningsSerialization() throws Exception
    {
        // Test warnings serialization in CasForwardResponse
        List<String> warnings = Arrays.asList("Warning 1", "Warning 2", "Test warning message");
        CasForwardResponse originalResponse = new CasForwardResponse((RowIterator) null, warnings);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        CasForwardResponse.serializer.serialize(originalResponse, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        CasForwardResponse deserializedResponse = CasForwardResponse.serializer.deserialize(in, 0);

        // Verify warnings are preserved
        assertTrue("Response should be successful", deserializedResponse.isSuccess());
        assertNotNull("Warnings should not be null", deserializedResponse.warnings);
        assertEquals("Warning count should match", warnings.size(), deserializedResponse.warnings.size());
        assertEquals("Warnings should match", warnings, deserializedResponse.warnings);
    }

    @Test
    public void testConsensusReadForwardResponseWarningsSerialization() throws Exception
    {
        // Test warnings serialization in ConsensusReadForwardResponse
        List<String> warnings = Arrays.asList("Read warning 1", "Read warning 2");
        ConsensusReadForwardResponse originalResponse = new ConsensusReadForwardResponse(
            (org.apache.cassandra.db.partitions.PartitionIterator) null, warnings);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        ConsensusReadForwardResponse.serializer.serialize(originalResponse, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        ConsensusReadForwardResponse deserializedResponse = ConsensusReadForwardResponse.serializer.deserialize(in, 0);

        // Verify warnings are preserved
        assertTrue("Response should be successful", deserializedResponse.isSuccess());
        assertNotNull("Warnings should not be null", deserializedResponse.warnings);
        assertEquals("Warning count should match", warnings.size(), deserializedResponse.warnings.size());
        assertEquals("Warnings should match", warnings, deserializedResponse.warnings);
    }
}