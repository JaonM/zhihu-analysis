package com.maqiang.dataprocess;

import com.maqiang.hbaseutil.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * Import user data from HDFS to HBase
 *
 * Created by maqiang on 16/02/2017.
 */
public class DataImport {

    private static final String USER_DATA = "zhihu_user";
    private static final String CF_INFO = "info";

    public static void importData(FileSystem fs,Path srcPath,HTable destTable) throws IOException {

        //遍历目录获取文件导入hbase
        FileStatus [] fileStatuses = fs.listStatus(srcPath);
        System.out.println("file count: "+fileStatuses.length);
        for(FileStatus fileStatus:fileStatuses) {
            if(fileStatus.isFile()) {
                System.out.println(fileStatus.getPath().getName());
                Path filePath = fileStatus.getPath();
                FSDataInputStream fds = fs.open(filePath);
                BufferedReader br = new BufferedReader(new InputStreamReader(fds));
                String readLine;
                String rowKey = "";
                while((readLine=br.readLine())!=null) {
//                    System.out.println(readLine);
                    String key;
                    String value;
                    try {
                        key = readLine.split("\t")[0];
                        value = readLine.split("\t")[1];
//                        System.out.println(key+"\t"+value);
                    }catch (Exception e) {
                        System.out.println("exception line: "+readLine);
                        continue;
                    }
                    if(key.equals("id")) {
                        rowKey = value;
                    } else {
                        if(!rowKey.equals("")) {
                            HBaseUtil.addRow(destTable,rowKey,CF_INFO,key,value);
                        }
                    }
                }
            }
        }
    }

    public static void main(String [] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
            Path parentPath = new Path("hdfs://localhost:9000/zhihu/user_data");
            HTable table = HBaseUtil.getTable(USER_DATA);
            DataImport.importData(fs,parentPath,table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
