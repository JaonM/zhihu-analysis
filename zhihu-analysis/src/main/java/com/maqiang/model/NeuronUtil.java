package com.maqiang.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 竞争层神经元工具类
 * Created by maqiang on 02/03/2017.
 */
public class NeuronUtil {

    //    public static final int WEIGHT_NUM = 2460600;
    public static final int WEIGHT_NUM = 10000;

    public static final String WEIGHT_PATH = "hdfs://localhost:9000/zhihu_mining/cluster/weight/weight.seq";

    /**
     * 随机初始化竞争层神经元
     * 通过SequenceFile格式保存
     * 竞争层神经元是一个10*10二维矩阵 拓扑结构为平面
     *
     * @param fs
     * @param path
     * @return
     */
    public static void initToFile(Configuration conf, FileSystem fs, String path) throws IOException {
        Path weightPath = new Path(path);
        if (fs.exists(weightPath)) {
            System.out.println("already exists");
            return;
        }
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, weightPath, key.getClass(), value.getClass());
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                key.set(i + "," + j);
                float[] vectors = new float[WEIGHT_NUM];
                StringBuilder sb = new StringBuilder();
                for (int k = 0; k < vectors.length; k++) {
                    vectors[k] = (float) (new Random().nextFloat() * Math.pow(-1, new Random().nextInt()));
//                    System.out.print(vectors[k] + " ");
                    sb.append(vectors[k]);
                    sb.append(",");
                }
//                System.out.println();
                String result = sb.toString();
                value.set(result.substring(0, result.length() - 1));
//                System.out.println(value.toString());
                writer.append(key, value);
            }
        }
        IOUtils.closeStream(writer);
    }

    public static List<Neuron> loadNeuronFromFile(Configuration conf, FileSystem fs, String path) throws IOException {
        if (!fs.exists(new Path(path))) {
            return null;
        }
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(path), conf);
        List<Neuron> neurons = new ArrayList<>();
        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
            int i = Integer.valueOf(key.toString().split(",")[0]);
            int j = Integer.valueOf(key.toString().split(",")[1]);
            String[] strs = value.toString().split(",");
            float[] vectors = new float[strs.length];
            for (int k = 0; k < strs.length; k++) {
                vectors[k] = Float.valueOf(strs[k]);
            }
            neurons.add(new Neuron(i, j, vectors));
        }
        IOUtils.closeStream(reader);

        return neurons;
    }

    public static Neuron readNeuron(Configuration conf, Text key) throws IOException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        Path path = new Path(WEIGHT_PATH);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text value = new Text();
        String[] keys = key.toString().split(",");
        reader.next(key, value);
        String[] vec = value.toString().split(",");
//        System.out.println(vec.length);
        float[] vectors = new float[vec.length];
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = Float.valueOf(vec[i]);
        }
        IOUtils.closeStream(reader);
        return new Neuron(Integer.valueOf(keys[0]), Integer.valueOf(keys[1]), vectors);

    }

    public static boolean updateWeightVector(FileSystem fs, Configuration conf, Path path, Neuron neuron) {
        boolean result = false;
        try {
            if (!fs.exists(path)) {
                return false;
            }
            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);

            Text key = new Text();
            Text value = new Text();
            key.set(neuron.getI() + "," + neuron.getJ());
            StringBuilder sb = new StringBuilder();
            for (float f : neuron.getWeights()) {
                sb.append(f);
                sb.append(",");
            }
            value.set(sb.toString().substring(0, sb.toString().length() - 1));
            writer.append(key, value);

            IOUtils.closeStream(writer);
            result = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void updateWeightVector(FileSystem fs,Configuration conf,Path path,List<Neuron> neurons) throws
        IOException {
        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
        for(Neuron neuron:neurons) {
            key.set(neuron.getI() + "," + neuron.getJ());
            float[] vectors = neuron.getWeights();
            StringBuilder sb = new StringBuilder();
            for (float f:vectors) {
                sb.append(f);
                sb.append(",");
            }
            String result = sb.toString();
            value.set(result.substring(0, result.length() - 1));
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
//            initToFile(conf, fs, WEIGHT_PATH);
//            Neuron neuron = readNeuron(conf, new Text("9,7"));
//            System.out.println(neuron.getI() + " " + neuron.getJ());
//            updateWeightVector(fs,conf,new Path(NeuronUtil.WEIGHT_PATH),new Neuron(19,10,new float[NeuronUtil.WEIGHT_NUM]));
            List<Neuron> neurons = loadNeuronFromFile(conf,fs,WEIGHT_PATH);
            System.out.println("neuron size: "+neurons.size() );
            for(Neuron neuron:neurons) {
                System.out.println(neuron.getI()+" "+neuron.getJ());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
