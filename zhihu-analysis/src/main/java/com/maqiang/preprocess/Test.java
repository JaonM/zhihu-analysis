package com.maqiang.preprocess;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by maqiang on 01/03/2017.
 */
public class Test {
    public static void main(String[] args) {
//        System.out.println(doubleX(2));
//        float [] f = new float[3];
//        for(float v :f) {
//            System.out.print(v);
//        }
        List<Integer> list = new ArrayList<>();
        doubleX(list);
        System.out.println(list.size());
    }

    public static void doubleX(List<Integer> list) {
        list.add(1);
    }
}
