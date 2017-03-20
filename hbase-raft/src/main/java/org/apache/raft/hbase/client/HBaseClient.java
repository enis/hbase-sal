/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.raft.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.raft.hbase.HBaseUtils;
import org.apache.ratis.client.RaftClient;

public class HBaseClient {

  private RaftClient raftClient;
  private HRegionInfo regionInfo;

  public HBaseClient (RaftClient raftClient) {
    this.raftClient = raftClient;
    this.regionInfo = HBaseUtils.createRegionInfo(HBaseUtils.createTableDescriptor());
  }

  public void put(List<Put> puts) throws IOException {
    MultiRequest request = buildRequest(buildActions(puts), regionInfo);
    MultiRequestMessage msg = new MultiRequestMessage(request);
    raftClient.send(msg);
  }

  private static List<Action> buildActions(List<Put> puts) {
    List<Action> actions = new ArrayList<>(puts.size());
    for (int i=0; i<puts.size(); i++) {
      actions.add(new Action(puts.get(i), i));
    }
    return actions;
  }

  private static MultiRequest buildRequest(List<Action> actions, HRegionInfo regionInfo)
      throws IOException {

    int countOfActions = actions.size();
    if (countOfActions <= 0) throw new DoNotRetryIOException("No Actions");
    MultiRequest.Builder multiRequestBuilder = MultiRequest.newBuilder();
    RegionAction.Builder regionActionBuilder = RegionAction.newBuilder();
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    MutationProto.Builder mutationBuilder = MutationProto.newBuilder();

    regionActionBuilder.setRegion(RequestConverter.buildRegionSpecifier(
      HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME,
        regionInfo.getEncodedNameAsBytes()));

    byte[] regionName = regionInfo.getRegionName();
    regionActionBuilder = RequestConverter.buildRegionAction(regionName, actions,
      regionActionBuilder, actionBuilder, mutationBuilder);

    multiRequestBuilder.addRegionAction(regionActionBuilder.build());

    return multiRequestBuilder.build();
  }
}
