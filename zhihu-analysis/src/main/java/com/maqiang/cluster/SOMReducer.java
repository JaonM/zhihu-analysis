package com.maqiang.cluster;

import com.maqiang.model.Neuron;
import com.maqiang.model.NeuronUtil;
import com.maqiang.model.VectorWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created by maqiang on 03/03/2017.
 */
public class SOMReducer extends Reducer<Text, VectorWritable, Text, Text> {

    @SuppressWarnings("Duplicates")
    @Override
    public void reduce(Text key, Iterable<VectorWritable> values, Context context) throws IOException,
            InterruptedException {
//        Neuron target = NeuronUtil.readNeuron(context.getConfiguration(), key);
        //计算平均改变向量
        float[] meanVector = meanVector(values);
//        for (float f : meanVector) {
//            System.out.print(f + " ");
//        }
//        System.out.println();

        //获取学习率
        float a = context.getConfiguration().getFloat("learnRate", 0.1f);

        //获取领域
        int N = context.getConfiguration().getInt("N", 0);
        System.out.println("area is: " + N);
        int i = Integer.valueOf(key.toString().split(",")[0]);
        int j = Integer.valueOf(key.toString().split(",")[1]);

        int startI = i - N;
        int startJ = j - N;

        int endI = i + N;
        int endJ = j + N;

        List<Neuron> neurons = NeuronUtil.loadNeuronFromFile(context.getConfiguration(),
                FileSystem.get(URI.create("hdfs://localhost:9000"), context.getConfiguration()),
                NeuronUtil.WEIGHT_PATH);
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), context.getConfiguration());
        if (N==0) {
            Neuron neuron = locateNeuron(i, j, neurons);
            if (neuron != null) {
                synchronized (neuron){
                    float[] weight = neuron.getWeights();
                    for (int k = 0; k < weight.length; k++) {
                        weight[k] += a * Math.exp(-N) * meanVector[k];
                    }
                    updateNeuron(neurons, weight, neuron);
                }
            }
        }else {
            for (int m = startI; m < endI; m++) {
                for (int n = startJ; n < endJ; n++) {
                    //改变正方形领域内权值
                    if (m > 0 && m < 10 && n > 0 && n < 10) {
//                    Neuron neuron = NeuronUtil.readNeuron(context.getConfiguration(), new Text(m + "," + n));
                        Neuron neuron = locateNeuron(m, n, neurons);
                        if (neuron == null) {
                            continue;
                        }
                        synchronized (neuron) {
                            float[] weight = neuron.getWeights();
                            for (int k = 0; k < weight.length; k++) {
                                weight[k] += a * Math.exp(-N) * meanVector[k];
                            }
                            updateNeuron(neurons, weight, neuron);
                        }
//                    neuron.setWeights(weight);
//                    NeuronUtil.updateWeightVector(fs, context.getConfiguration(), new Path(NeuronUtil.WEIGHT_PATH), neuron);
                    }
                }
            }
        }
        NeuronUtil.updateWeightVector(fs, context.getConfiguration(), new Path(NeuronUtil.WEIGHT_PATH), neurons);
        context.write(new Text(key.toString()), new Text("OK finished"));

    }


    /**
     * 计算平均改变向量
     *
     * @param values
     * @return
     */
    private float[] meanVector(Iterable<VectorWritable> values) {
        float[] result = new float[NeuronUtil.WEIGHT_NUM];
        int size = 0;
        for (VectorWritable vectorWritable : values) {
            size++;
            for (int i = 0; i < vectorWritable.get().length; i++) {
                result[i] += vectorWritable.get()[i].get();
            }
        }
        for (int i = 0; i < result.length; i++) {
            result[i] = result[i] / size;
        }
        return result;
    }

    private Neuron locateNeuron(int i, int j, List<Neuron> neurons) {
        for (Neuron neuron : neurons) {
            if (neuron.getI() == i && neuron.getJ() == j) {
                return neuron;
            }
        }
        return null;
    }

    private void updateNeuron(List<Neuron> neurons, float[] weight, Neuron neuron) {
        for (Neuron n : neurons) {
            if (n.getI() == neuron.getI() && n.getJ() == neuron.getJ()) {
                n.setWeights(weight);
            }
        }
    }
}
