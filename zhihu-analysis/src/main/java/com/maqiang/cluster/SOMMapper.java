package com.maqiang.cluster;

import com.maqiang.model.Neuron;
import com.maqiang.model.NeuronUtil;
import com.maqiang.model.UserNode;
import com.maqiang.model.VectorWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * SOM Mapper
 * 找到获胜神经元
 * Created by maqiang on 03/03/2017.
 */
public class SOMMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        if(value.toString().equals("")) {
            return;
        }
        //初始化竞争层和输入层
        List<Neuron> neurons = NeuronUtil.loadNeuronFromFile(context.getConfiguration(),
                FileSystem.get(URI.create("hdfs://localhost:9000"), context.getConfiguration()),
                NeuronUtil.WEIGHT_PATH);

        if (neurons == null) {
            System.err.println("初始化竞争层失败");
            return;
        }

        UserNode userNode = UserNode.initUserNode(value.toString());

        //权值向量和输入向量归一化
        for (Neuron neuron : neurons) {
            neuron.setWeights(normalize(neuron.getWeights()));
        }
        userNode.setVectors(normalize(userNode.getVectors()));

        Neuron winNeuron = winner(userNode, neurons);

        FloatWritable[] floatWritables = new FloatWritable[userNode.getVectors().length];
        for (int i = 0; i < userNode.getVectors().length; i++) {
            floatWritables[i] = new FloatWritable(userNode.getVectors()[i] - winNeuron.getWeights()[i]);
        }

        VectorWritable vectorWritable = new VectorWritable(floatWritables);

        System.out.println(winNeuron.getI() + " " + winNeuron.getJ());
        context.write(new Text(winNeuron.getI() + "," + winNeuron.getJ()), vectorWritable);
    }

    /**
     * 向量归一化
     *
     * @param vectors
     * @return
     */
    public static float[] normalize(float[] vectors) {
        float distance = distance(vectors);
        for (int i = 0; i < vectors.length; i++) {
            vectors[i] = vectors[i] / distance;
        }
        return vectors;
    }

    /**
     * 计算向量距离
     *
     * @param vectors
     * @return
     */
    public static float distance(float[] vectors) {
        float sum = 0;
        for (float f : vectors) {
            sum += f * f;
        }
        return (float) Math.sqrt(sum);
    }

    /**
     * 计算两向量余弦相似度
     * 由于向量已经归一化，只需要计算点积
     *
     * @param v1
     * @param v2
     * @return
     */
    public static float similarity(float[] v1, float[] v2) {
        if (v1.length != v2.length) {
            return 0;
        }
        float sum = 0;
        for (int i = 0; i < v1.length; i++) {
            sum += v1[i] * v2[i];
        }
        return sum;
    }

    /**
     * 找出获胜神经元
     */
    public static Neuron winner(UserNode userNode, List<Neuron> neurons) {
        int winIndex = 0;
        float max = Integer.MIN_VALUE;
        for (int i = 0; i < neurons.size(); i++) {
            float sim = similarity(userNode.getVectors(), neurons.get(i).getWeights());
//            System.out.println("sim: "+sim);
//            System.out.println("max: "+max);
            if (sim > max) {
//                System.out.println("here");
                max = sim;
                winIndex = i;
            }
        }
//        System.out.println("winner Index: "+winIndex);
        return neurons.get(winIndex);
    }
}
