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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by maqiang on 05/03/2017.
 */
public class ClusterProcess {

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
