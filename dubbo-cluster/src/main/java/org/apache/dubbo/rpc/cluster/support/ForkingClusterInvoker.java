/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FORKS;
import static org.apache.dubbo.rpc.cluster.Constants.FORKS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * NOTICE! This implementation does not work well with async call.
 *
 * Invoke a specific number of invokers concurrently, usually used for demanding real-time operations, but need to waste more service resources.
 *
 * <a href="http://en.wikipedia.org/wiki/Fork_(topology)">Fork</a>
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link org.apache.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     */
    private final ExecutorService executor = Executors.newCachedThreadPool(
            new NamedInternalThreadFactory("forking-cluster-timer", true));
    // 线程池推荐用 ThreadPoolExecutor 初始化；
//    Executor executor = new ThreadPoolExecutor(4, 20, 100, TimeUnit.SECONDS,
//                           new PriorityBlockingQueue<Runnable>(),
//                           new NamedInternalThreadFactory("forking-cluster-timer", true));
    public ForkingClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {
            checkInvokers(invokers, invocation);
            final List<Invoker<T>> selected;
            // 并行数量，默认是 2
            final int forks = getUrl().getParameter(FORKS_KEY, DEFAULT_FORKS);
            // 调用超时， 默认是 1s
            final int timeout = getUrl().getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
            // 选出要调用的节点列表；当并行数配置超过调用实例数量，则默认为调用实例数
            if (forks <= 0 || forks >= invokers.size()) {
                selected = invokers;
            } else {
                selected = new ArrayList<>();
                for (int i = 0; i < forks; i++) {
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);
                    if (!selected.contains(invoker)) {
                        //Avoid add the same invoker several times.
                        selected.add(invoker);
                    }
                }
            }
            // 将选中的实例添加到上下文中
            RpcContext.getContext().setInvokers((List) selected);
            final AtomicInteger count = new AtomicInteger();
            // 使用阻塞队列来存放调用实例的执行结果
            final BlockingQueue<Object> ref = new LinkedBlockingQueue<>();
            for (final Invoker<T> invoker : selected) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Result result = invoker.invoke(invocation);
                            ref.offer(result);
                        } catch (Throwable e) {
                            int value = count.incrementAndGet();
                            // 根据队列先入先出原则，因为该算法是只要有一个调用成功就好，
                            // 所以只有当全部的调用实例都失败，才记录到队列中，因为下面取结果会判断是不是异常
                            if (value >= selected.size()) {
                                ref.offer(e);
                            }
                        }
                    }
                });
            }
            try {
                Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);
                // 没有一个实例执行成功，抛出异常
                if (ret instanceof Throwable) {
                    Throwable e = (Throwable) ret;
                    throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : 0, "Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e.getCause() != null ? e.getCause() : e);
                }
                return (Result) ret;
            } catch (InterruptedException e) {
                throw new RpcException("Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
            }
        } finally {
            // clear attachments which is binding to current thread.
            RpcContext.getContext().clearAttachments();
        }
    }
}
