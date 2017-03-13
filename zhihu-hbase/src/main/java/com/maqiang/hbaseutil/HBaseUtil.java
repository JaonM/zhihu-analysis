package com.maqiang.hbaseutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by maqiang on 14/02/2017.
 */
public class HBaseUtil {

    private static Configuration conf = HBaseConfiguration.create();

    public static void createTable(String tableName,String [] columnFamilies) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if(hAdmin.tableExists(tableName)) {
            System.out.println("table is already existed");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for(String cf:columnFamilies) {
                tableDesc.addFamily(new HColumnDescriptor(cf));
            }
            hAdmin.createTable(tableDesc);
            System.out.println("create table successfully");
        }
    }

    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        if(hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("table deleted");
        }else {
            System.out.println("table is not existed");
        }
    }

    public static HTable getTable(String tableName) throws IOException {
        return new HTable(conf,tableName);
    }

    /**
     *
     * @param table 表名
     * @param rowKey    行主键
     * @param columnFamily  列簇名
     * @param column    列名
     * @param value 值
     * @throws IOException
     */
    //添加数据
    public static void addRow(HTable table,String rowKey,String columnFamily,String column,String value)
            throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
    }

    //删除一行数据
    public static void deleteRow(HTable table,String rowKey) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);

    }

    //删除多条数据
    public static void deleteMultiRows(String tableName,String [] rowKeys) throws IOException {
        HTable table = new HTable(conf,tableName);
        List<Delete> delList = new ArrayList<Delete>();
        for (String rowKey:rowKeys) {
            Delete del = new Delete(Bytes.toBytes(rowKey));
            delList.add(del);
        }
        table.delete(delList);
    }

    public static void getRow(HTable hTable,String rowKey) throws IOException {
        Get get =  new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);
        for(KeyValue rowKV:result.raw()) {
            System.out.println("Row Name: " + new String(rowKV.getRow()) + " ");
            System.out.println("Timestamp: " + rowKV.getTimestamp() + " ");
            System.out.println("Column Family: " + new String(rowKV.getFamily()) + " ");
            System.out.println("Column Name:  " + new String(rowKV.getQualifier()) + " ");
            System.out.println("Value: " + new String(rowKV.getValue()) + " ");
        }
    }

    public static List<Cell> getRow(HTable table,String rowKey,String columnFamily,String qualifier) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return result.getColumnCells(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));
    }


    public static ResultScanner getAllRows(HTable hTable) throws IOException {
        Scan scan =new Scan();
        return hTable.getScanner(scan);
    }


    public static void main(String [] args) {
        try {
//            HBaseUtil.createTable("test",new String [] {"cf"});
//            HBaseUtil.deleteTable("test");
//            HBaseUtil.createTable("zhihu_user",new String [] {"info","attrs"});
            HTable table = HBaseUtil.getTable("zhihu_user");
            List<Cell> cells = HBaseUtil.getRow(table,"8ee009e55dc6e5b57e3db80d51ecaaab","attrs",
                    "keywords");
            System.out.println("length: "+cells.size());
            for (Cell cell:cells) {
//                System.out.println("cell: "+Bytes.toString(CellUtil.cloneValue(cell)));
                String str = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(str.split(",").length);
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}
