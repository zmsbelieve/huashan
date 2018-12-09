package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        //获取客户端对象
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
        //创建目录
        fs.mkdirs(new Path("/0592/dashen"));
        //关闭资源
        fs.close();
        System.out.println("over");
    }

    /**
     * 文件上传
     * 参数优先级:
     * 代码>resources目录下的hdfs-site.xml>集群环境下的hdfs-site.xml>default配置
     */
    @Test
    public void testCopyFromLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        //上传API
        fs.copyFromLocalFile(new Path("e:/bangzhagn.txt"), new Path("/bangzhang.txt"));
        fs.close();
    }

    /**
     * 文件下载
     */
    @Test
    public void testCopyToLocal() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        //下载API
        //fs.copyToLocalFile(new Path("/bangzhang.txt"), new Path("e:/bangzhang.txt"));
        //第一个参数 是否删除源文件  第四个参数 ture就不会生成crc校验文件
        fs.copyToLocalFile(true, new Path("/bangzhang.txt"), new Path("e:/bangzhang.txt"), true);
        fs.close();
    }

    /**
     * 删除文件
     *
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        //执行删除
        fs.delete(new Path("/0529"), true);
        fs.close();
    }

    /**
     * 文件更名
     */
    @Test
    public void testRename() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        //重命名
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));
        fs.close();
    }

    /**
     * 查看文件详情
     */
    @Test
    public void testFileList() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            //文件名称
            System.out.println(file.getPath().getName());
            //文件权限
            System.out.println(file.getPermission());
            //文件长度
            System.out.println(file.getLen());
            //文件块信息
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts)
                    //主机名
                    System.out.println(host);
            }
            System.out.println("-------------------------------");
        }
        fs.rename(new Path("/banzhang.txt"), new Path("/yanjing.txt"));
        fs.close();
    }
    //判断是文件还是文件夹
    @Test
    public void testListStatus() throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hdfs102:9000"), conf, "atguigu");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()){
                //文件
                System.out.println("f:" + fileStatus.getPath().getName());
            }else{
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
        fs.close();
    }
}
