package org.apache.raft.hbase;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.net.NetUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys.Server;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;

public class HBaseRaftServer {

  private final int port;
  private RaftServer raftServer;
  private RaftProperties properties;

  private HBaseRaftServer(String id, String[] servers) throws IOException {
    properties = new RaftProperties();
    //properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);
    RaftPeerId myId = RaftPeerId.getRaftPeerId(id);

    List<RaftPeer> peers = Arrays.stream(servers).map(addr -> new RaftPeer(
        RaftPeerId.getRaftPeerId(addr), addr))
        .collect(Collectors.toList());

    this.port = NetUtils.createSocketAddr(id).getPort();

    String idForPath = URLEncoder.encode(id, "UTF-8");

    properties.set(RaftServerConfigKeys.STORAGE_DIR_KEY,
        "/tmp/raft-server-" + idForPath);

    properties.setInt(Server.PORT_KEY, port);

    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);

    raftServer = RaftServer.newBuilder()
        .setServerId(myId)
        .setPeers(peers)
        .setProperties(properties)
        .setStateMachine(new RegionStateMachine())
        .build();
  }

  public void start() {
    raftServer.start();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: HBaseRaftServer <quorum> <id>");
      System.exit(1);
    }

    String[] servers = args[0].split(",");
    String id = args[1];

    HBaseRaftServer server = new HBaseRaftServer(id, servers);
    server.start();
  }
}
