# 手动启动并检查 node0

1. 在项目根目录运行 example：

   ```bash
   cargo run --example start_node
   ```

2. 记下输出中的 `Expected node0 ID`，将 `Started container` 的值填入：

   ```bash
   container_id='粘贴容器ID'
   ```

3. 查询 Docker 自动分配的 RPC 地址：

   ```bash
   rpc_address=$(docker port "$container_id" 26657/tcp)
   echo "$rpc_address"
   ```

4. 查询 RPC，确认 `node_id` 与 `Expected node0 ID` 一致：

   ```bash
   curl -fsS --max-time 3 "http://$rpc_address/status" |
     jq '.result | {node_id: .node_info.id, height: .sync_info.latest_block_height}'
   ```

   刚启动时可能连接失败，稍等后重试。只启动一个 validator，高度为 `0` 正常。

5. 如果一直无法访问，查看日志：

   ```bash
   docker logs --tail 50 "$container_id"
   ```

6. 测试结束后，停止节点、保存日志，再删除容器及匿名卷：

   ```bash
   docker stop "$container_id"
   docker logs "$container_id" > "${container_id}.log" 2>&1
   docker rm -v "$container_id"
   ```

   宿主机上的配置文件和已保存的日志不会被删除。
