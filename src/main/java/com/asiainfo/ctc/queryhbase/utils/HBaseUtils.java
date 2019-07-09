package com.asiainfo.ctc.queryhbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HBaseUtils {
    private static volatile Configuration conf = null;//volatitle防止指令重排序
    private static ExecutorService executor = Executors.newFixedThreadPool(10);
    private static volatile Connection connection = null;
    private static volatile Table table = null;

    private static Configuration getConf(){
        if(conf == null){
            synchronized (HBaseUtils.class){
                if(conf == null){
                    conf = HBaseConfiguration.create();
                }
            }
        }
        return conf;
    }

    public static Connection getConnection() throws IOException{
        if(connection == null){
            synchronized (HBaseUtils.class) {
                if (connection == null) {
                    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
                    conf = getConf();
                    conf.set("hadoop.security.authentication", "Kerberos");
                    conf.set("keytab.file", "/home/ocdp/ocdp.keytab");
                    conf.set("kerberos.principal", "ocdp/nn1@BJ.CTC");
                    conf.set("hbase.master.kerberos.principal", "hbase/_HOST@BJ.CTC");
                    conf.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@BJ.CTC");
                    conf.set("hbase.zookeeper.quorum", "dn002,dn012,dn022");
                    conf.set("hbase.zookeeper.property.clientPort", "2181");
                    conf.set("hbase.master", "nn1:16000");
                    conf.set("zookeeper.znode.parent", "/hbase-secure");
                    UserGroupInformation.setConfiguration(HBaseUtils.conf);
                    UserGroupInformation.loginUserFromKeytab("ocdp/nn1@BJ.CTC", "/home/ocdp/ocdp.keytab");
                    connection = ConnectionFactory.createConnection(HBaseUtils.conf, executor);
                    System.out.println("==============================connection:" + connection);
                }
            }
        }
        return connection;
    }

/*    public static Table getTable() throws IOException {
        connection = getConnection();
        if(table == null) {
            synchronized (HBaseUtils.class){
                if(table == null) {
                    table = connection.getTable(TableName.valueOf("interface_eda:testBulkLoad"));
                    System.out.println("==============================create table:" + table);
                }
            }
        }
        System.out.println("==============================return table:" + table);
        return table;
    }*/


}
