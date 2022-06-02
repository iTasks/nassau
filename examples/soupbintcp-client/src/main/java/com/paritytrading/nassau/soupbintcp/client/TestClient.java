/*
 * Copyright 2014 Nassau authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.paritytrading.nassau.soupbintcp.client;

import com.paritytrading.nassau.MessageListener;
import com.paritytrading.nassau.soupbintcp.SoupBinTCP;
import com.paritytrading.nassau.soupbintcp.SoupBinTCPClient;
import com.paritytrading.nassau.soupbintcp.SoupBinTCPClientStatusListener;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.HdrHistogram.Histogram;

class TestClient implements Closeable, MessageListener {

    private SoupBinTCPClient transport;

    private Histogram histogram;

    private int receiveCount;

    public static void main(String[] args) throws IOException {
        if (args.length != 4)
            usage();

        try {
            String host = args[0];
            int port = Integer.parseInt(args[1]);
            int packets = Integer.parseInt(args[2]);
            int packetsPerSecond = Integer.parseInt(args[3]);

            main(new InetSocketAddress(host, port), packets, packetsPerSecond);
        } catch (NumberFormatException e) {
            usage();
        }
    }
    private final int USERNAME_LENGTH = 6;
    private final int PASSWORD_LENGTH = 6;
    private final int SESSION_LENGTH = 10;
    private final int SEQUENCE_NUMBER_LENGTH = 20;
    private final int LOGIN_REQUEST_LENGTH = 49;
    private final int LOGIN_LENGTH = USERNAME_LENGTH + PASSWORD_LENGTH + SESSION_LENGTH + SEQUENCE_NUMBER_LENGTH;
    private final int LOGIN_ACCEPT_LENGTH = SESSION_LENGTH + SEQUENCE_NUMBER_LENGTH;
    private final ByteBuffer txPayload = ByteBuffer.allocateDirect(LOGIN_REQUEST_LENGTH);

    public void login(SoupBinTCP.LoginRequest payload) throws IOException {
        txPayload.clear();
        payload.put(txPayload);
        txPayload.flip();
        transport.send(txPayload);
    }

    private static void main(InetSocketAddress address, int packets, int packetsPerSecond) throws IOException {
        try (final TestClient client = TestClient.connect(address)) {
            tryLoginAfterConnect(client);
            long intervalNanos = 1_000_000_000 / packetsPerSecond;

            ByteBuffer buffer = ByteBuffer.allocateDirect(Long.BYTES);

            System.out.println("Warming up...");

            client.sendAndReceive(buffer, packets, intervalNanos);

            client.reset();

            System.out.println("Benchmarking...");

            client.sendAndReceive(buffer, packets, intervalNanos);

            System.out.printf("Results (n = %d)\n", packets);
            System.out.printf("\n");
            System.out.printf("      Min: %10.2f µs\n", client.histogram.getMinValue() / 1000.0);
            System.out.printf("   50.00%%: %10.2f µs\n", client.histogram.getValueAtPercentile(50.00) / 1000.0);
            System.out.printf("   90.00%%: %10.2f µs\n", client.histogram.getValueAtPercentile(90.00) / 1000.0);
            System.out.printf("   99.00%%: %10.2f µs\n", client.histogram.getValueAtPercentile(99.00) / 1000.0);
            System.out.printf("   99.90%%: %10.2f µs\n", client.histogram.getValueAtPercentile(99.90) / 1000.0);
            System.out.printf("   99.99%%: %10.2f µs\n", client.histogram.getValueAtPercentile(99.99) / 1000.0);
            System.out.printf("  100.00%%: %10.2f µs\n", client.histogram.getValueAtPercentile(100.00) / 1000.0);
            System.out.printf("\n");
        }
    }

    private static void tryLoginAfterConnect(TestClient client) throws IOException {
        SoupBinTCP.LoginRequest payload = new SoupBinTCP.LoginRequest();
        payload.setUsername("IIFCS1");
        payload.setPassword("");
        payload.setRequestedSession("TEST");
        payload.setRequestedSequenceNumber(1L);
        client.login(payload);
    }



    private static TestClient connect(SocketAddress address) throws IOException {
        SocketChannel channel = SocketChannel.open();

        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.connect(address);
        channel.configureBlocking(false);

        return new TestClient(channel);
    }

    private TestClient(SocketChannel channel) {
        transport = new SoupBinTCPClient(channel, this, new SoupBinTCPClientStatusListener() {

            @Override
            public void loginAccepted(SoupBinTCPClient session, SoupBinTCP.LoginAccepted payload) {
                System.out.println("Login Session: " + session + " Payload: " + payload);
            }

            @Override
            public void loginRejected(SoupBinTCPClient session, SoupBinTCP.LoginRejected payload) {
                System.out.println("Login Reject Session: " + session + " Payload: " + payload);
            }

            @Override
            public void endOfSession(SoupBinTCPClient session) {
                System.out.println("End Session: " + session);
            }

            @Override
            public void heartbeatTimeout(SoupBinTCPClient session) {
                System.out.println("Heartbeat Session: " + session);
            }

        });

        histogram = new Histogram(3);
    }

    @Override
    public void message(ByteBuffer buffer) {
        histogram.recordValue(System.nanoTime() - buffer.getLong());
        System.out.println("Message: " + Arrays.toString(buffer.array()));
        receiveCount++;
    }

    @Override
    public void close() throws IOException {
        System.out.println("Closing....: ");
        transport.close();
    }

    private void sendAndReceive(ByteBuffer buffer, int packets, long intervalNanos) throws IOException {
        for (long sendAtNanos = System.nanoTime(); receiveCount < packets; transport.receive()) {
            if (System.nanoTime() >= sendAtNanos) {
                buffer.clear();

                buffer.putLong(sendAtNanos);

                buffer.flip();

                transport.send(buffer);

                sendAtNanos += intervalNanos;
            }
        }
    }

    private void reset() {
        histogram.reset();

        receiveCount = 0;
    }

    private static void usage() {
        System.err.println("Usage: nassau-soupbintcp-client <host> <port> <packets> <packets-per-second>");
        System.exit(2);
    }

}
