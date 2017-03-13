package com.maqiang.model;

/**
 * SOM竞争层神经元
 * Created by maqiang on 01/03/2017.
 */
public class Neuron {

    //竞争层索引
    private int i;
    private int j;

    //权值向量
    private float[] weights;

    public Neuron(int i, int j, float[] weights) {
        this.i = i;
        this.j = j;
        this.weights = weights;
    }

    public Neuron(int i, int j) {
        this.i = i;
        this.j = j;
        this.weights = new float[2460600];
    }

    public void setI(int i) {
        this.i = i;
    }

    public void setJ(int j) {
        this.j = j;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    public int getI() {
        return i;
    }

    public int getJ() {
        return j;
    }

    public float[] getWeights() {
        return weights;
    }

}
