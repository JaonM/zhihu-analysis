package com.maqiang.cluster;

import com.maqiang.model.Cluster;
import com.maqiang.model.Neuron;
import com.maqiang.model.NeuronUtil;
import com.maqiang.model.UserNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by maqiang on 05/03/2017.
 */
public class ClusterProcess {

    public static void main(String [] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
            Path path = new Path("hdfs://localhost:9000/zhihu_mining/cluster/result/result.txt");
            if(fs.exists(path)) {
                fs.createNewFile(path);
            }
            List<Cluster> clusters = process(conf,fs);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
            for(Cluster cluster:clusters) {
                for(UserNode userNode:cluster.getUserNodes()) {
                    bw.write(userNode.getName()+"\t");
                }
                bw.write("\n");
            }
            bw.flush();
            bw.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Cluster> process(Configuration conf, FileSystem fs) throws IOException {
        List<Neuron> neurons = NeuronUtil.loadNeuronFromFile(conf, fs, NeuronUtil.WEIGHT_PATH);
        List<UserNode> userNodes = loadUser(fs);

        List<Cluster> clusters = new ArrayList<>();

        for (Neuron neuron : neurons) {
            neuron.setWeights(SOMMapper.normalize(neuron.getWeights()));
            Cluster cluster = new Cluster(neuron);
            clusters.add(cluster);
        }

        for (UserNode userNode : userNodes) {
            userNode.setVectors(SOMMapper.normalize(userNode.getVectors()));

            Neuron winNeuron = SOMMapper.winner(userNode, neurons);
            for (Cluster cluster : clusters) {
                if (cluster.getNeuron() == winNeuron) {
                    cluster.getUserNodes().add(userNode);
                }
            }
        }


        return clusters;
    }

    private static List<UserNode> loadUser(FileSystem fs) throws IOException {
        Path path = new Path(SOMDriver.INPUT_PATH);
        FSDataInputStream fis = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        List<UserNode> userNodes = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            if (!line.equals("")) {
                UserNode node = UserNode.initUserNode(line);
                userNodes.add(node);
            }
        }
        IOUtils.closeStream(fis);
        br.close();
        return userNodes;
    }
}
