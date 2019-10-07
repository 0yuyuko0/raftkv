package com.yuyuko.raftkv.raft.read;

import com.yuyuko.raftkv.raft.RaftException;
import com.yuyuko.raftkv.raft.core.Message;

import java.util.*;

public class ReadOnly {
    // 使用entry的数据为key，保存当前pending的readIndex状态
    private Map<String, ReadIndexStatus> pendingReadIndex = new HashMap<>();

    // 保存entry的数据为的队列，pending的readindex状态在这个队列中进行排队
    private List<String> readIndexQueue = new ArrayList<>();

    /**
     * 从readonly队列中返回最后一个数据
     */
    public String lastPendingRequestCtx() {
        if (readIndexQueue.size() == 0)
            return "";
        return readIndexQueue.get(readIndexQueue.size() - 1);
    }

    public void addRequest(long index, Message m) {
        String ctx = new String(m.getEntries().get(0).getData());
        // 判断是否重复添加
        if (pendingReadIndex.containsKey(ctx))
            return;
        pendingReadIndex.put(ctx, new ReadIndexStatus(m, index));
        readIndexQueue.add(ctx);
    }

    public int recvAck(Message m) {
        // 根据context内容到map中进行查找
        ReadIndexStatus readIndexStatus =
                pendingReadIndex.get(new String(Optional.ofNullable(m.getContext()).orElse("".getBytes())));
        // 找不到就返回，说明这个HB消息没有带上readindex相关的数据
        if (readIndexStatus == null)
            return 0;
        // 将该消息的ack set加上该应答节点
        readIndexStatus.getAcks().add(m.getFrom());
        // 返回当前ack的节点数量，+1是因为包含了leader
        return readIndexStatus.getAcks().size() + 1;
    }

    /**
     * 在确保某HB消息被集群中半数以上节点应答了，此时尝试在readindex队列中查找，
     * 看一下队列中的readindex数据有哪些可以丢弃了（也就是已经被应答过了）
     * 最后返回被丢弃的数据队列
     *
     * @param m
     * @return
     */
    public List<ReadIndexStatus> advance(Message m) {
        String ctx = new String(m.getContext());

        List<ReadIndexStatus> rss = new ArrayList<>();
        boolean found = false;

        int i = 0;
        for (String okCtx : readIndexQueue) {
            i++;
            ReadIndexStatus rs = pendingReadIndex.get(okCtx);
            if (rs == null)
                // 不可能出现在map中找不到该数据的情况
                throw new RaftException("cannot find corresponding read state from pending map");
            // 都加入应答队列中，后面用于根据这个队列的数据来删除pendingReadIndex中对应的数据
            rss.add(rs);
            if (okCtx.equals(ctx)) {
                found = true;
                break;
            }
        }
        if (found) {
            // 找到了，就丢弃在这之前的队列readonly数据了
            readIndexQueue = new ArrayList<>(readIndexQueue.subList(i, readIndexQueue.size()));
            rss.forEach(rs -> pendingReadIndex.remove(new String(rs.getRequest().getEntries().get(0).getData())));
            return rss;
        }
        return new ArrayList<>();
    }

    public Map<String, ReadIndexStatus> getPendingReadIndex() {
        return pendingReadIndex;
    }

    public List<String> getReadIndexQueue() {
        return readIndexQueue;
    }
}
