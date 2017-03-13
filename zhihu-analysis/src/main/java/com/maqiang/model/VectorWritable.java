package com.maqiang.model;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by maqiang on 03/03/2017.
 */
public class VectorWritable implements WritableComparable<VectorWritable> {

    private FloatWritable[] vectors;
    private int num;

    public VectorWritable() {
//        vectors = new FloatWritable[NeuronUtil.WEIGHT_NUM];
    }

    public VectorWritable(FloatWritable[] vectors) {
        this.vectors = vectors;
        this.num = vectors.length;
    }

    public void set(FloatWritable[] vectors) {
        this.vectors = vectors;
    }

    public FloatWritable[] get() {
        return vectors;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(num);
        for (FloatWritable floatWritable : vectors) {
            floatWritable.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num = dataInput.readInt();
        vectors = new FloatWritable[num];
        for(int i =0;i<num;i++) {
            vectors[i]=new FloatWritable();
            vectors[i].readFields(dataInput);
        }
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (FloatWritable floatWritable : vectors) {
            hashCode += floatWritable.hashCode();
        }
        return hashCode;
    }


    @Override
    public int compareTo(VectorWritable vectorWritable) {
        return 0;
    }
}
