package com.maqiang.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by maqiang on 05/03/2017.
 */
public class Cluster {

    private List<UserNode> userNodes = new ArrayList<>();

    private Neuron neuron;

    public Cluster(Neuron neuron) {
        this.neuron = neuron;
    }

    public Neuron getNeuron() {
        return neuron;
    }

    public List<UserNode> getUserNodes() {
        return userNodes;
    }
}
