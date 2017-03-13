package com.maqiang.model;

/**
 *
 * 神经网络输入节点
 * Created by maqiang on 01/03/2017.
 */
public class UserNode {

    private String name;

    private float[] vectors;

    public UserNode(String name, float[] vectors) {
        this.name = name;
        this.vectors = vectors;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setVectors(float[] vectors) {
        this.vectors = vectors;
    }

    public String getName() {
        return name;
    }

    public float[] getVectors() {
        return vectors;
    }

    /**
     * 从文本一行初始化输入节点
     * @param src
     * @return
     */
    public static UserNode initUserNode(String src) {
        try {
            String[] strs = src.split("\t");
            String name = strs[0];
            String[] vectors = strs[1].split(",");
            float[] vec = new float[NeuronUtil.WEIGHT_NUM];
            for (int i = 0; i < vectors.length; i++) {
                vec[i] = Float.valueOf(vectors[i]);
            }
            return new UserNode(name,vec);
        }catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
