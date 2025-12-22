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

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.KeyspaceParams;

public class RemoteClientStateTest
{
    private static final String KEYSPACE1 = "RemoteClientStateTest";

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1));
    }

    @Test
    public void testRemoteClientStateFromLocalState() throws Exception
    {
        // Create a local client state with various attributes
        ClientState localState = ClientState.forInternalCalls();
        localState.setKeyspace(KEYSPACE1);

        // Convert to remote client state
        RemoteClientState remoteState = RemoteClientState.from(localState);

        // Verify attributes are preserved
        assertEquals("Keyspace should be preserved", KEYSPACE1, remoteState.getRawKeyspace());
        assertEquals("Keyspace should be preserved", KEYSPACE1, remoteState.getKeyspace());
        assertEquals("isSuper should be preserved", localState.isSuper(), remoteState.isSuper());
        assertEquals("isSystem should be preserved", localState.isSystem(), remoteState.isSystem());
        assertEquals("applyGuardrails should be preserved", localState.applyGuardrails(), remoteState.applyGuardrails());
    }

    @Test
    public void testRemoteClientStateSerialization() throws Exception
    {
        // Create a local client state
        ClientState localState = ClientState.forInternalCalls();
        localState.setKeyspace(KEYSPACE1);
        RemoteClientState originalState = RemoteClientState.from(localState);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        RemoteClientState.serializer.serialize(originalState, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        RemoteClientState deserializedState = RemoteClientState.serializer.deserialize(in, 0);

        // Verify attributes match
        assertEquals("Keyspace should match after serialization", originalState.getRawKeyspace(), deserializedState.getRawKeyspace());
        assertEquals("isSuper should match after serialization", originalState.isSuper(), deserializedState.isSuper());
        assertEquals("isSystem should match after serialization", originalState.isSystem(), deserializedState.isSystem());
        assertEquals("applyGuardrails should match after serialization", originalState.applyGuardrails(), deserializedState.applyGuardrails());
    }

    @Test
    public void testRemoteClientStateNullKeyspaceSerialization() throws Exception
    {
        // Create a client state without keyspace
        ClientState localState = ClientState.forInternalCalls();
        // Don't set keyspace (should be null)
        RemoteClientState originalState = RemoteClientState.from(localState);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        RemoteClientState.serializer.serialize(originalState, out, 0);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        RemoteClientState deserializedState = RemoteClientState.serializer.deserialize(in, 0);

        // Verify null keyspace is handled correctly
        assertNull("Keyspace should be null", deserializedState.getRawKeyspace());
    }

    @Test(expected = InvalidRequestException.class)
    public void testRemoteClientStateGetKeyspaceThrowsOnNull()
    {
        // Create a remote client state without keyspace
        ClientState localState = ClientState.forInternalCalls();
        RemoteClientState remoteState = RemoteClientState.from(localState);

        // Should throw InvalidRequestException when trying to get keyspace
        remoteState.getKeyspace();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoteClientStateThrowsOnGetUser()
    {
        ClientState localState = ClientState.forInternalCalls();
        RemoteClientState remoteState = RemoteClientState.from(localState);

        // Should throw UnsupportedOperationException
        remoteState.getUser();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoteClientStateThrowsOnEnsurePermission()
    {
        ClientState localState = ClientState.forInternalCalls();
        RemoteClientState remoteState = RemoteClientState.from(localState);

        // Should throw UnsupportedOperationException - using IResource version
        IResource resource = null;
        Permission permission = null;
        remoteState.ensurePermission(permission, resource);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoteClientStateThrowsOnSetKeyspace()
    {
        ClientState localState = ClientState.forInternalCalls();
        RemoteClientState remoteState = RemoteClientState.from(localState);

        // Should throw UnsupportedOperationException
        remoteState.setKeyspace("test");
    }

    @Test
    public void testSerializedSizeCalculation() throws Exception
    {
        // Test with keyspace
        ClientState localState = ClientState.forInternalCalls();
        localState.setKeyspace(KEYSPACE1);
        RemoteClientState remoteStateWithKs = RemoteClientState.from(localState);

        // Test without keyspace
        ClientState localStateNoKs = ClientState.forInternalCalls();
        RemoteClientState remoteStateNoKs = RemoteClientState.from(localStateNoKs);

        // Calculate expected sizes
        long expectedSizeWithKs = RemoteClientState.serializer.serializedSize(remoteStateWithKs, 0);
        long expectedSizeNoKs = RemoteClientState.serializer.serializedSize(remoteStateNoKs, 0);

        // Verify sizes are reasonable
        assertTrue("Size with keyspace should be larger", expectedSizeWithKs > expectedSizeNoKs);
        assertTrue("Size should include boolean flags", expectedSizeNoKs >= 4); // At least 4 booleans

        // Verify actual serialization size matches calculated size
        DataOutputBuffer out = new DataOutputBuffer();
        RemoteClientState.serializer.serialize(remoteStateWithKs, out, 0);

        assertEquals("Calculated size should match actual serialized size",
                    expectedSizeWithKs, out.getLength());
    }
}