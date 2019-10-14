package com.yuyuko.raftkv.remoting.peer;

import com.yuyuko.raftkv.remoting.peer.client.NettyPeerClient;
import com.yuyuko.raftkv.remoting.peer.client.NettyPeerClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public class PeerReconnectionManager {
    private static final Logger log = LoggerFactory.getLogger(PeerReconnectionManager.class);

    private final ScheduledExecutorService reconnectScheduler;

    private final int maxReconnectTimes;

    private final Map<Long, Integer> reconnectTimes = new ConcurrentHashMap<>();

    private final Map<Long, ScheduledFuture> reconnectFutures = new ConcurrentHashMap<>();

    private final Set<Long> doNotReconnectNodeIds = ConcurrentHashMap.newKeySet();

    private final int reconnectPeriodSeconds;

    public PeerReconnectionManager(NettyPeerClientConfig peerConfig) {
        this.reconnectScheduler =
                new ScheduledThreadPoolExecutor(peerConfig.getReconnectThreadPoolSize(),
                        r -> new Thread(r, "ReconnectScheduler"));
        this.maxReconnectTimes = peerConfig.getMaxReconnectTimes();
        this.reconnectPeriodSeconds = peerConfig.getReconnectPeriodSeconds();
    }

    public boolean reconnect(long peerId) {
        if (doNotReconnectNodeIds.contains(peerId)) {
            doNotReconnectNodeIds.remove(peerId);
            return false;
        }
        ScheduledFuture<?> future = reconnectScheduler.scheduleAtFixedRate(() -> {
            try {
                NettyPeerClient.getGlobalInstance().reconnect(peerId);
                resetReconnectTimes(peerId);
            } catch (Throwable ex) {
                int reconnectTimes = this.reconnectTimes.getOrDefault(peerId, 0) + 1;
                log.warn("[Reconnect Failed],failed {} times", reconnectTimes, ex);
                this.reconnectTimes.put(peerId, reconnectTimes);
                if (reconnectTimes >= maxReconnectTimes) {
                    log.warn("[Stop Reconnect],reconnect times over the maxReconnectTimes");
                    if (reconnectFutures.get(peerId) != null)
                        reconnectFutures.remove(peerId).cancel(true);
                }
            }
        }, 0, reconnectPeriodSeconds, TimeUnit.SECONDS);
        reconnectFutures.put(peerId, future);
        return true;
    }

    public void doNotReconnect(long peerId) {
        doNotReconnectNodeIds.add(peerId);
    }

    public void resetReconnectTimes(long peerId) {
        reconnectTimes.put(peerId, 0);
    }
}