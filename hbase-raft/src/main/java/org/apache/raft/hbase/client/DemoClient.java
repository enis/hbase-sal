package org.apache.raft.hbase.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.raft.hbase.HBaseUtils;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.RaftClientSenderWithGrpc;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftConfiguration;

import com.google.common.collect.Lists;

public class DemoClient {

  private void run(String[] servers, int numRows) throws IOException {
    RaftProperties properties = new RaftProperties();
    List<RaftPeer> peers = Arrays.stream(servers).map(addr -> new RaftPeer(addr, addr))
        .collect(Collectors.toList());
    RaftConfiguration raftConfiguration = RaftConfiguration.newBuilder().setConf(peers).build();
    RaftClientSenderWithGrpc requestSender = new RaftClientSenderWithGrpc(peers);

    RaftClient client = new RaftClientImpl("demo-client", peers,
        requestSender, null, properties);

    final HBaseClient hbaseClient = new HBaseClient(client);

    for (int i = 0; i < numRows; i++) {
      byte[] b = Bytes.toBytes(i);
      hbaseClient.put(Lists.newArrayList(
          new Put(b).addImmutable(HBaseUtils.FAMILY, b, b)));
    }

    Threads.sleep(2000);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: HBaseRaftServer <quorum> <numRows>");
      System.exit(1);
    }

    String[] servers = args[0].split(",");
    int numRows = Integer.valueOf(args[1]);

    new DemoClient().run(servers, numRows);
  }

}
