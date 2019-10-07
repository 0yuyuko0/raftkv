package com.yuyuko.raftkv.server.raft;

import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.ResponseCode;
import com.yuyuko.raftkv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ReadIndexMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.raftkv.remoting.server.ClientRequest;
import com.yuyuko.raftkv.remoting.server.ClientResponse;
import com.yuyuko.raftkv.remoting.server.NettyRequestProcessor;
import com.yuyuko.raftkv.remoting.server.NettyServer;
import com.yuyuko.utils.concurrent.Chan;
import io.netty.channel.ChannelHandlerContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftKV implements NettyRequestProcessor {
    private Chan<byte[]> proposeChan;

    private Chan<byte[]> readIndexChan;

    private Map<String, String> map = new ConcurrentHashMap<>();

    private static volatile RaftKV globalInstance;

    public RaftKV(Chan<byte[]> proposeChan, Chan<byte[]> commitChan,
                  Chan<byte[]> readIndexChan, Chan<ReadState> readStateChan) {
        this.proposeChan = proposeChan;
        this.readIndexChan = readIndexChan;
        Thread thread1 = new Thread(() -> readCommits(commitChan));
        thread1.setName("RaftKVApplied");
        thread1.start();
        Thread thread2 = new Thread(() -> readReadStates(readStateChan));
        thread2.setName("RaftKVReadState");
        thread2.start();
        globalInstance = this;
    }

    public static RaftKV getInstance() {
        return globalInstance;
    }

    @Override
    public void processRequest(ClientRequest clientRequest, ChannelHandlerContext ctx) {
        if (clientRequest.getCode() == RequestCode.PROPOSE)
            propose(clientRequest);
        else
            readIndex(clientRequest);
    }

    public void propose(ClientRequest request) {
        this.proposeChan.send(ProtostuffCodec.getInstance().encode(request));
    }

    public void readIndex(ClientRequest request) {
        this.readIndexChan.send(ProtostuffCodec.getInstance().encode(request));
    }

    private void readCommits(Chan<byte[]> commitChan) {
        byte[] data;
        while ((data = commitChan.receive()) != null) {
            ClientRequest request = ProtostuffCodec.getInstance().decode(data,
                    ClientRequest.class);
            ProposeMessage proposeMessage = ProtostuffCodec.getInstance().decode(request.getBody()
                    , ProposeMessage.class);
            map.put(proposeMessage.getKey(), proposeMessage.getValue());
            NettyServer.getGlobalInstance().sendResponseToClient(request.getRequestId(),
                    new ClientResponse(ResponseCode.PROPOSE,
                            "Propose Success".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void readReadStates(Chan<ReadState> readStateChan) {
        ReadState readState;
        while ((readState = readStateChan.receive()) != null) {
            byte[] data = readState.getRequestCtx();
            ClientRequest request = ProtostuffCodec.getInstance().decode(data,
                    ClientRequest.class);
            ReadIndexMessage readIndexMessage =
                    ProtostuffCodec.getInstance().decode(request.getBody(),
                            ReadIndexMessage.class);
            NettyServer.getGlobalInstance().sendResponseToClient(request.getRequestId(),
                    new ClientResponse(ResponseCode.READ_INDEX,
                            map.getOrDefault(readIndexMessage.getKey(), "(null)").getBytes(StandardCharsets.UTF_8)));
        }
    }


}
