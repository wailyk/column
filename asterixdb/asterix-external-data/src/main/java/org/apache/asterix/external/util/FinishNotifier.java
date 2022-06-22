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
package org.apache.asterix.external.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Map;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FinishNotifier {
    private static final String ADDRESS_KEY = "notifier_address";
    private static final String PORT_KEY = "notifier_port";
    private final int partition;
    private final String address;
    private final int port;

    public FinishNotifier(Map<String, String> configuration, int partition) throws HyracksDataException {
        address = configuration.get(ADDRESS_KEY);
        port = Integer.parseInt(configuration.get(PORT_KEY));
        try (Socket socket = new Socket(address, port)) {
            final OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream());
            out.write(partition + "\n");
            out.flush();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }

        this.partition = partition;
    }

    public void notifyClient() throws IOException {
        try (Socket socket = new Socket(address, port)) {
            final OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream());
            out.write(partition + "\n");
            out.flush();
        }
    }

    public static boolean isNotifierConfiguerd(Map<String, String> configuration) {
        return configuration.containsKey(ADDRESS_KEY) && configuration.containsKey(PORT_KEY);
    }
}
