package com.maqiang.cluster;

import com.maqiang.model.NeuronUtil;
import com.maqiang.model.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;

/**
 * Created by maqiang on 03/03/2017.
 */
public class SOMDriver extends Configured implements Tool {

    public static final String INPUT_PATH = "hdfs://localhost:9000/zhihu_mining/cluster/input/user_vector.txt";
    private static final String OUTPUT_PATH = "hdfs://localhost:9000/zhihu_mining/cluster/output";
    private static final String ARG_PATH = "hdfs://localhost:9000/zhihu_mining/cluster/arg/arg.txt";

    private static final int MAX_ROUND = 500;

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);

        //初始化竞争层权值
        String weightPath = NeuronUtil.WEIGHT_PATH;
        NeuronUtil.initToFile(conf, fs, weightPath);

        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        //读取参数
        Path argPath = new Path(ARG_PATH);
        if (!fs.exists(argPath)) {
            fs.createNewFile(argPath);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(argPath)));
        String line;
        float learnRate = 0.7f;
        int N = 7;

        int round = 50;
        while ((line = br.readLine()) != null) {
            learnRate = Float.valueOf(line.split("\t")[0]);
            N = Integer.valueOf(line.split("\t")[1]);
            if (line.split("\t").length > 2) {
                round = Integer.valueOf(line.split("\t")[2]);
            }
        }

        br.close();

        boolean result = true;
        for (int n = round; n < MAX_ROUND && n <= round + 5; n++) {
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            System.out.println("round is: " + n);
            conf.setFloat("learnRate", learnRate);
            conf.setInt("N", N);
            Job job = Job.getInstance(conf);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(VectorWritable.class);
            job.setMapperClass(SOMMapper.class);
            job.setReducerClass(SOMReducer.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            result = result && job.waitForCompletion(true);
            if (!result) {
                break;
            }

            learnRate = learnRate * (1 - (float) n / MAX_ROUND);
            if(learnRate<5.038334E-7) {
                learnRate = (float) 5.038334E-7;
            }
            N = Math.round(N * (1 - (float) n / (MAX_ROUND)));

        }
        if(learnRate<5.038334E-7) {
            learnRate = (float) 5.038334E-7;
        }

        round += 5;
        //更新参数
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(argPath)));
        bw.write(learnRate + "\t" + N + "\t" + round);
        bw.flush();
        bw.close();

//        if (fs.exists(outputPath)) {
//            fs.delete(outputPath, true);
//        }
//        conf.setFloat("learnRate", learnRate);
//        conf.setInt("N", N);
//        Job job = Job.getInstance(conf);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(VectorWritable.class);
//        job.setMapperClass(SOMMapper.class);
//        job.setReducerClass(SOMReducer.class);
//
//        FileInputFormat.addInputPath(job, inputPath);
//        FileOutputFormat.setOutputPath(job, outputPath);
//        result=result&&job.waitForCompletion(true);
        return result ? 0 : 1;

    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new SOMDriver(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
