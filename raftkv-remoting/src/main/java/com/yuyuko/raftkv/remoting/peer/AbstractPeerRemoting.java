package com.yuyuko.raftkv.remoting.peer;

public abstract class AbstractPeerRemoting {
    protected final long id;

    protected final PeerMessageProcessor messageProcessor;

    protected final PeerChannelManager channelManager;

    public AbstractPeerRemoting(long id, PeerMessageProcessor messageProcessor,
                                PeerChannelManager channelManager) {
        this.id = id;
        this.messageProcessor = messageProcessor;
        this.channelManager = channelManager;
    }

    public abstract void start();

    public abstract void shutdown();

    public long getId() {
        return id;
    }
}
