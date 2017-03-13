package com.maqiang.preprocess;

import com.ansj.vec.Word2VEC;
import com.maqiang.hbaseutil.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 生成用户训练输入向量
 * Created by maqiang on 01/03/2017.
 */
public class PreProcess {

    private static final String TABLE = "zhihu_user";

    private static final String USER_VECTOR = "hdfs://localhost:9000/zhihu_mining/cluster/input/user_vector.txt";

    private static Word2VEC w2c;

    public static Word2VEC getW2C() {
        try {
            if (w2c == null) {
                w2c = new Word2VEC();
                w2c.loadJavaModel(PreProcess.class.getClassLoader().getResource("wiki_vector.mod").getFile());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return w2c;
    }

    /**
     * 返回向量最大维度
     */
    public static int maxDimension() {
        int dim = 0;
        try {
            ResultScanner scanner = HBaseUtil.getAllRows(HBaseUtil.getTable(TABLE));
            Word2VEC w2c = getW2C();
            for (Result result : scanner) {
                String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
                String keyword = Bytes.toString(result.getValue(Bytes.toBytes("attrs"), Bytes.toBytes("keywords")));
                String[] words = keyword.split(",");
                int sum = 0;
                for (String word : words) {
                    if (w2c.getWordVector(word) != null) {
                        sum++;
                    }
                }
//                System.out.println(name + " " + sum);
                if (sum > dim) {
                    dim = sum;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dim;
    }

    private static Float[] constructVector(String[] words) {
        List<Float> valueList = new ArrayList<>();
        Word2VEC word2VEC = getW2C();
        for (String word : words) {
            float[] wordVector = word2VEC.getWordVector(word);
            if (wordVector != null) {
                for (float v : wordVector) {
                    valueList.add(v);
                }
            }
        }
        return  valueList.toArray(new Float[valueList.size()]);
//        float[] inputVector = new float[valueList.size()];
//
//        for (int i = 0; i < inputVector.length; i++) {
////            if (i >= valueList.size()) {
////                inputVector[i] = 0;
////            } else {
////                inputVector[i] = valueList.get(i);
////            }
//            inputVector[i] = valueList.get(i);
//        }
//        return inputVector;
    }

    public static void vector2HDFS(FileSystem fs, Path path) throws IOException {
//        int dim = maxDimension();
//        System.out.println("max dimension: " + dim);
        if(!fs.exists(path)) {
            fs.createNewFile(path);
        }

        FSDataOutputStream out = fs.append(path);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        ResultScanner scanner = HBaseUtil.getAllRows(HBaseUtil.getTable("zhihu_user"));
        for (Result result : scanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            String keyword = Bytes.toString(result.getValue(Bytes.toBytes("attrs"), Bytes.toBytes("keywords")));
            String[] words = keyword.split(",");
            if (words.length > 1) {
                Float[] vectors = constructVector(words);
                StringBuffer sb = new StringBuffer();
                for (float f : vectors) {
//                        System.out.print(f+" ");
                    sb.append(f);
                    sb.append(",");
                }
                System.out.println("writing " + name + " " + vectors.length + " to HDFS");
                bw.write(name + "\t" + sb.toString().substring(0, sb.toString().length() - 1) + "\n\r");
            }
        }
        bw.flush();
        bw.close();
        IOUtils.closeStream(out);
    }

    public static void loadUserVector(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FSDataInputStream fis = fs.open(new Path(path));
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line;
        while ((line = br.readLine()) != null) {
           if(!line.equals("")) {
               String[] strs = line.split("\t");
               String name = strs[0];
//        System.out.println(name);
               String[] vectors = strs[1].split(",");
               List<Float> vectorList = new ArrayList<>();
               for (String vector : vectors) {
                   vectorList.add(Float.valueOf(vector));
               }
           }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
            vector2HDFS(fs, new Path(USER_VECTOR));
        } catch (IOException e) {
            e.printStackTrace();
        }
//
//        try {
//            loadUserVector("hdfs://localhost:9000/zhihu_mining/cluster/input/user_vectors.txt");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
