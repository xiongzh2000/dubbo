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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.cluster.merger.MergerFactory;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.MERGER_KEY;

/**
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);
    private ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("mergeable-cluster-executor", true));

    public MergeableClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        // 获取 merger 配置参数值
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), MERGER_KEY);
        // s1, 没有配置 merger,
        if (ConfigUtils.isEmpty(merger)) { // If a method doesn't have a merger, only invoke one Group
            // 如果方法不需要Merge，退化为只调一个group即可--选择第一个有效的Invoker调用并返回结果
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    try {
                        return invoker.invoke(invocation);
                    } catch (RpcException e) {
                        if (e.isNoInvokerAvailableAfterFilter()) {
                            log.debug("No available provider for service" + directory.getUrl().getServiceKey() + " on group " + invoker.getUrl().getParameter(GROUP_KEY) + ", will continue to try another group.");
                        } else {
                            throw e;
                        }
                    }
                }
            }
            //如果没有任意Invoker满足isAvailable(), 那么尝试调用第一个Invoker(多尝试一下, 多一次机会)
            return invokers.iterator().next().invoke(invocation);
        }


        //S2, 异步调用不同分组下所有服务；
        Class<?> returnType;
        try {
            returnType = getInterface().getMethod(
                    invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }

        Map<String, Result> results = new HashMap<>();
        // 异步调用所有的实例，并把对象存储到 results 中,key为 [{group}/{interfaceName}:{version}]
        for (final Invoker<T> invoker : invokers) {
            RpcInvocation subInvocation = new RpcInvocation(invocation, invoker);
            //todo xyds ，异步调用的实现的封装
            subInvocation.setAttachment(ASYNC_KEY, "true");
            //todo 后面一次是异常 是否会覆盖前面一次的正确结果；
            results.put(invoker.getUrl().getServiceKey(), invoker.invoke(subInvocation));
        }

        Object result = null;

        List<Result> resultList = new ArrayList<Result>(results.size());

        for (Map.Entry<String, Result> entry : results.entrySet()) {
            Result asyncResult = entry.getValue();
            try {
                //即阻塞等待线程执行完成
                Result r = asyncResult.get();
                if (r.hasException()) {
                    //如果异步执行有异常(包括超时), 那么输出error级别的日志, 不影响最终的结果(只是部分数据缺失)
                    log.error("Invoke " + getGroupDescFromServiceKey(entry.getKey()) +
                                    " failed: " + r.getException().getMessage(),
                            r.getException());
                } else {
                    resultList.add(r);
                }
            } catch (Exception e) {
                throw new RpcException("Failed to invoke service " + entry.getKey() + ": " + e.getMessage(), e);
            }
        }

        // S3 对结果合并；
        if (resultList.isEmpty()) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        } else if (resultList.size() == 1) {
            //如果只有一个结果, 那么直接返回即可
            return resultList.iterator().next();
        }

        if (returnType == void.class) {
            //如果返回类型为void, 那么new一个result为null的RpcResult返回即可
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        }

        // S3.1  . 开头，例如merger=".addAll", 这段逻辑就是调用结果类型的原生方法, 例如服务的返回结果是List<Long>，即list类型，那么merger=".addAll"就是调用List集合的.addAll()
        if (merger.startsWith(".")) {
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException e) {
                throw new RpcException("Can not merge result because missing method [ " + merger + " ] in class [ " +
                        returnType.getClass().getName() + " ]");
            }
            if (!Modifier.isPublic(method.getModifiers())) {
                method.setAccessible(true);
            }
            result = resultList.remove(0).getValue();
            try {
                //如果merger=".addAll"指定的方法返回类型不为void，且和dubbo服务接口方法返回类型是相同类型
                //.addAll()返回类型是boolean，而dubbo服务接口方法返回类型是List
                if (method.getReturnType() != void.class
                        && method.getReturnType().isAssignableFrom(result.getClass())) {
                    for (Result r : resultList) {
                        result = method.invoke(result, r.getValue());
                    }
                } else {
                    for (Result r : resultList) {
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RpcException("Can not merge result: " + e.getMessage(), e);
            }
        } else {
            Merger resultMerger;
            //true和default都是默认值(大小写不敏感)
            if (ConfigUtils.isDefault(merger)) {
                //dubbo服务接口方法返回类型中查找merger配置的方法, SPI org/apache/dubbo/rpc/cluster/Merger.java
                resultMerger = MergerFactory.getMerger(returnType);
            } else {
                //自定义merger实现
                resultMerger = ExtensionLoader.getExtensionLoader(Merger.class).getExtension(merger);
            }
            if (resultMerger != null) {
                List<Object> rets = new ArrayList<Object>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }
                result = resultMerger.merge(
                        rets.toArray((Object[]) Array.newInstance(returnType, 0)));
            } else {
                throw new RpcException("There is no merger to merge result.");
            }
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }


    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public URL getUrl() {
        return directory.getUrl();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return "group [ " + key.substring(0, index) + " ]";
        }
        return key;
    }
}
