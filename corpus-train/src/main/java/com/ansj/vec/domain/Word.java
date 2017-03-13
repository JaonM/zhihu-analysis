package com.ansj.vec.domain;

import java.util.Random;

/**
 * Created by maqiang on 10/03/2017.
 */
public class Word {

    public String name;
    public double len;
    public double freq;
    public double[] syn0;
    public double[] syn1neg;

    public Word(int layerSize) {
        syn0 = new double[layerSize];
        syn1neg = new double[layerSize];

        Random random = new Random();
        for (int i = 0; i < syn0.length; i++) {
            syn0[i] = (random.nextDouble() - 0.5) / layerSize;
        }
    }
}
