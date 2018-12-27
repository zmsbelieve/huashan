package com.atguigu.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private String connectionString = "192.168.5.131:2181,192.168.5.132:2181,192.168.5.131:2183";
    private int sessionTimeout = 2000;//单位是ms
    private ZooKeeper zooKeeper;


    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        DistributeClient distributeClient = new DistributeClient();
        //获取zookeeper集群连接
        distributeClient.connect();
        //注册监听
        distributeClient.listen();
        //业务逻辑...
        distributeClient.business();
    }

    private void connect() throws IOException {
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    listen();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void listen() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/servers", true);
        ArrayList<String> hosts = new ArrayList<>();
        for(String child : children){
            byte[] data = zooKeeper.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        System.out.println(hosts);
    }

    private void business() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }
}
