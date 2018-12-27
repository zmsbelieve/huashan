package com.atguigu.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {

    private String connectionString = "192.168.5.131:2181,192.168.5.132:2181,192.168.5.131:2183";
    private int sessionTimeout = 2000;//单位是ms
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer distributeServer = new DistributeServer();
        //1.连接zookeeper集群
        distributeServer.connect();
        //2.注册服务
        distributeServer.regist(args[0]);
        //3.业务逻辑
        distributeServer.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }

    private void regist(String hostName) throws KeeperException, InterruptedException {
        zooKeeper.create("/servers/server",hostName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostName + " is online!");
    }

    private void connect() throws IOException {
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}
