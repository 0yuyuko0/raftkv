package com.yuyuko.raftkv.server;

import com.yuyuko.raftkv.raft.node.ConfChange;
import com.yuyuko.raftkv.raft.read.ReadState;
import com.yuyuko.raftkv.raft.utils.Tuple;
import com.yuyuko.raftkv.remoting.peer.NettyPeer;
import com.yuyuko.raftkv.remoting.peer.NettyPeerConfig;
import com.yuyuko.raftkv.remoting.peer.PeerNode;
import com.yuyuko.raftkv.remoting.server.NettyServerConfig;
import com.yuyuko.raftkv.server.raft.RaftKV;
import com.yuyuko.raftkv.server.raft.RaftNode;
import com.yuyuko.raftkv.server.server.Server;
import com.yuyuko.utils.concurrent.Chan;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Startup {
    public static void main(String[] args) throws ParseException {
        CommandLine commandLine = parseCmdLine("raftkv", args);
        if (commandLine == null)
            System.exit(-1);

        Server server = createServer(commandLine);
        server.start();

    }

    private static Server createServer(CommandLine commandLine) {
        long id = Long.parseLong(commandLine.getOptionValue("id"));
        List<String> peers = Arrays.asList(commandLine.getOptionValue("c").split(","));

        Chan<byte[]> proposeChan = new Chan<>();

        Chan<ConfChange> confChangeChan = new Chan<>();

        Chan<byte[]> readIndexChan = new Chan<>();

        Tuple<Chan<byte[]>, Chan<ReadState>> tuple = RaftNode.newRaftNode(id, peers,
                proposeChan, readIndexChan,
                confChangeChan);
        Chan<byte[]> commitChan = tuple.getFirst();
        Chan<ReadState> readStateChan = tuple.getSecond();

        new RaftKV(proposeChan, commitChan, readIndexChan, readStateChan);

        NettyServerConfig config = new NettyServerConfig();

        if (commandLine.hasOption("p"))
            config.setListenPort(Integer.parseInt(commandLine.getOptionValue("p")));

        Server server = new Server(config);
        server.init(id);

        List<PeerNode> peerNodes = new ArrayList<>();
        for (int i = 0; i < peers.size(); i++) {
            String addr = peers.get(i);
            String[] strs = addr.split(":");
            String ip = strs[0];
            int port = Integer.parseInt(strs[1]);
            peerNodes.add(new PeerNode(i + 1, ip, port));
        }

        NettyPeer nettyPeer = new NettyPeer(new NettyPeerConfig(), peerNodes);
        nettyPeer.connectToPeers(id);

        return server;
    }

    private static final CommandLine parseCmdLine(String appName, String[] args) throws ParseException {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth(110);
        Options options = buildCommandlineOptions(new Options());
        CommandLine commandLine = null;
        try {
            commandLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            helpFormatter.printHelp(appName, options, true);
        }
        return commandLine;
    }

    private static final Options buildCommandlineOptions(Options options) {
        Option port = new Option("p", "port", true, "server port");
        port.setRequired(false);
        options.addOption(new Option("i", "id", true, "raft node id"))
                .addOption(new Option("c", "cluster", true,
                        "raft server address list, eg: 192.168.0.1:9876,192.168.0.2:9876"))
                .addOption(port);
        return options;
    }
}
