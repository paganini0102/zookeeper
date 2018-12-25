/**
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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    /** Zookeeper服务器 */
    private final ZooKeeperServer zks;
    /** 请求队列 */
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    /** 下个处理器 */
    private final RequestProcessor nextProcessor;
    /** 快照处理线程 */
    private Thread snapInProcess = null;
    /** 是否在运行中 */
    volatile private boolean running;

    /**
     * 等待被刷新到磁盘的请求队列
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    /** 随机数生成器 */
    private final Random r = new Random(System.nanoTime());
    /**
     * 快照个数
     * The number of log entries to log before starting a snapshot
     */
    private static int snapCount = ZooKeeperServer.getSnapCount();
    /** 结束请求标识 */
    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    @Override
    public void run() {
        try {
            // 写日志数量初始化为0
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 确保所有的服务器在同一时间不是使用的同一个快照
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) { // 没有需要刷新到磁盘的请求
                    si = queuedRequests.take(); // 从请求队列中取出一个请求，若队列为空会阻塞
                } else {
                    si = queuedRequests.poll(); // 从请求队列中取出一个请求，若队列为空，则返回空，不会阻塞
                    if (si == null) {
                        flush(toFlush); // 刷新到磁盘
                        continue;
                    }
                }
                if (si == requestOfDeath) { // 在关闭处理器之后，会添加requestOfDeath，表示关闭后不再处理请求
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    if (zks.getZKDatabase().append(si)) { // 将请求添加至日志文件，只有事务性请求才会返回true
                        logCount++; // 写入一条日志，logCount加1
                        if (logCount > (snapCount / 2 + randRoll)) { // 满足roll the log的条件
                            randRoll = r.nextInt(snapCount/2);
                            // roll the log
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) { // 正在进行快照
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") { // 创建线程来处理快照
                                        public void run() {
                                            try {
                                                zks.takeSnapshot(); // 进行快照
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) { // 等待被刷新到磁盘的请求队列为空
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        // 查看此时toFlush是否为空，如果为空，说明近段时间读多写少，直接响应
                        if (nextProcessor != null) {
                            // 下个处理器开始处理请求
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush(); // 刷新到磁盘
                            }
                        }
                        continue;
                    }
                    toFlush.add(si); // 将请求添加至被刷新至磁盘队列
                    if (toFlush.size() > 1000) { // 队列大小大于1000，直接刷新到磁盘
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally{
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    /**
     * 刷新到磁盘
     * @param toFlush
     * @throws IOException
     * @throws RequestProcessorException
     */
    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        zks.getZKDatabase().commit(); // 提交至ZK数据库
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i); // 下个处理器开始处理请求
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
