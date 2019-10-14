package com.yuyuko.raftkv.server.raft;

import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.remoting.protocol.RequestCode;
import com.yuyuko.raftkv.remoting.protocol.ResponseCode;
import com.yuyuko.raftkv.remoting.protocol.body.ProposeMessage;
import com.yuyuko.raftkv.remoting.protocol.body.ReadIndexMessage;
import com.yuyuko.raftkv.remoting.protocol.codec.ProtostuffCodec;
import com.yuyuko.raftkv.remoting.server.*;
import com.yuyuko.raftkv.server.server.Server;
import com.yuyuko.selector.Channel;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RaftKV implements ClientRequestProcessor {
    private final Channel<byte[]> proposeChan;

    private final Channel<byte[]> readIndexChan;

    private final Map<String, String> map = new ConcurrentHashMap<>();

    private static volatile RaftKV globalInstance;

    public RaftKV(Channel<byte[]> proposeChan, Channel<byte[]> commitChan,
                  Channel<byte[]> readIndexChan, Channel<ReadState> readStateChan) {
        this.proposeChan = proposeChan;
        this.readIndexChan = readIndexChan;
        Thread thread1 = new Thread(() -> readCommits(commitChan));
        thread1.setName("RaftKVApplied");
        thread1.start();
        Thread thread2 = new Thread(() -> readReadStates(readStateChan));
        thread2.setName("RaftKVReadState");
        thread2.start();
    }

    @Override
    public void processRequest(ClientRequest clientRequest) {
        if (clientRequest.getCode() == RequestCode.PROPOSE)
            propose(clientRequest);
        else
            readIndex(clientRequest);
    }

    public void propose(ClientRequest request) {
        this.proposeChan.write(ProtostuffCodec.getInstance().encode(request));
    }

    public void readIndex(ClientRequest request) {
        this.readIndexChan.write(ProtostuffCodec.getInstance().encode(request));
    }

    private void readCommits(Channel<byte[]> commitChan) {
        byte[] data;
        while ((data = commitChan.read()) != null) {
            ClientRequest request = ProtostuffCodec.getInstance().decode(data,
                    ClientRequest.class);
            ProposeMessage proposeMessage = ProtostuffCodec.getInstance().decode(request.getBody()
                    , ProposeMessage.class);
            map.put(proposeMessage.getKey(), proposeMessage.getValue());
            Server.sendResponseToClient(request.getRequestId(),
                    new ClientResponse(ResponseCode.PROPOSE,
                            "Propose Success".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void readReadStates(Channel<ReadState> readStateChan) {
        ReadState readState;
        while ((readState = readStateChan.read()) != null) {
            byte[] data = readState.getRequestCtx();
            ClientRequest request = ProtostuffCodec.getInstance().decode(data,
                    ClientRequest.class);
            ReadIndexMessage readIndexMessage =
                    ProtostuffCodec.getInstance().decode(request.getBody(),
                            ReadIndexMessage.class);
            Server.sendResponseToClient(request.getRequestId(),
                    new ClientResponse(ResponseCode.READ_INDEX,
                            map.getOrDefault(readIndexMessage.getKey(), "(null)").getBytes(StandardCharsets.UTF_8)));
        }
    }


}
