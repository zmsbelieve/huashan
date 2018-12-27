package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {

//    private String connectionString = "192.168.5.131:2181,192.168.5.132:2181,192.168.5.131:2183";
    private String connectionString = "192.168.5.151:2181,192.168.5.152:2181,192.168.5.151:2183";
    private int sessionTimeout = 2000;//单位是ms
    private ZooKeeper zooKeeper;

    /**
     * 获取客户端
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
//                List<String> children = null;
//                try {
//                    System.out.println("**********start**********");
//                    //注意这里设为true则可以一直监听
//                    children = zooKeeper.getChildren("/", true);
//                    children.stream().forEach(System.out::println);
//                    System.out.println("**********end**********");
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }

            }
        });
    }

    /**
     * 创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        //Ids 这个是用来控制权限的 一般用OPEN_ACL_UNSAFE
        String path = zooKeeper.create("/atguigu", "dahaigezuishuai".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * 获取节点信息并监听
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/", true);
        children.stream().forEach(System.out::println);

        //睡眠
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists("/test", false);
        System.out.println(exists == null ? "not exist" : "exist");
    }
    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/dstAttack", false, null);
        System.out.println(new String(data));
    }
}
