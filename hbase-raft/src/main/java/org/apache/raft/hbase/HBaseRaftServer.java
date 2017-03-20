package org.apache.raft.hbase;

import static org.apache.ratis.grpc.RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY;
import static org.apache.ratis.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY;
import static org.apache.ratis.server.RaftServerConfigKeys.RAFT_SERVER_STORAGE_DIR_KEY;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.net.NetUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.RaftGRpcService;
import org.apache.ratis.grpc.server.PipelinedLogAppenderFactory;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.LogAppenderFactory;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerImpl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HBaseRaftServer {

  private final int port;
  private RaftServer raftServer;
  private RaftProperties properties;

  private HBaseRaftServer(String id, String[] servers) throws IOException {
    properties = new RaftProperties();
    properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);


    List<RaftPeer> peers = Arrays.stream(servers).map(addr -> new RaftPeer(addr, addr))
        .collect(Collectors.toList());
    Preconditions.checkArgument(Lists.newArrayList(servers).contains(id),
        "%s is not one of %s specified in %s", id, servers);
    RaftConfiguration raftConfiguration = RaftConfiguration.newBuilder().setConf(peers).build();

    this.port = NetUtils.createSocketAddr(id).getPort();

    String idForPath = URLEncoder.encode(id, "UTF-8");

    properties.set(RAFT_SERVER_STORAGE_DIR_KEY,
        "/tmp/raft-server-" + idForPath);
    properties.setInt(RAFT_GRPC_SERVER_PORT_KEY, port);

    properties.setClass(RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);

    raftServer = new RaftServerImpl(id, raftConfiguration, properties,
        new RegionStateMachine());
  }

  public void start() {
    RaftGRpcService grpcService = new RaftGRpcService(raftServer, properties);
    grpcService.start();
    raftServer.setServerRpc(grpcService);
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
