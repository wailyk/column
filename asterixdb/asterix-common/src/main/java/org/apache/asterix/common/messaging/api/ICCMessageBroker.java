/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.common.messaging.api;

import java.util.List;

import org.apache.hyracks.api.messages.IMessageBroker;

public interface ICCMessageBroker extends IMessageBroker {
    enum ResponseState {
        UNINITIALIZED,
        SUCCESS,
        FAILURE
    }

    /**
     * Sends the passed message to the specified {@code nodeId}
     *
     * @param msg
     * @param nodeId
     * @throws Exception
     */
    boolean sendApplicationMessageToNC(INcAddressedMessage msg, String nodeId) throws Exception;

    /**
     * Sends the passed message to the specified {@code nodeId}
     *
     * @param msg
     * @param nodeId
     * @throws Exception
     */
    boolean sendRealTimeApplicationMessageToNC(INcAddressedMessage msg, String nodeId) throws Exception;

    /**
     * Sends the passed requests to all NCs and wait for the response
     *
     * @param ncs
     * @param requests
     * @param timeout
     * @param realTime
     * @throws Exception
     */
    Object sendSyncRequestToNCs(long reqId, List<String> ncs, List<? extends INcAddressedMessage> requests,
            long timeout, boolean realTime) throws Exception;

    /**
     * respond to a sync request
     *
     * @param reqId
     * @param response
     */
    void respond(Long reqId, INcResponse response);

    /**
     * @return a new request id
     */
    long newRequestId();
}
