package com.maqiang.preprocess;

import com.ansj.vec.Word2VEC;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 通过MapReduce对用户特征维度排序
 * 以获得最大维度统一模型输入维度
 * Created by maqiang on 01/03/2017.
 */
public class MaxDimension extends Configured implements Tool {

    public static class DimensionSortMapper extends TableMapper<IntWritable,Text> {

        private static Word2VEC w2c = new Word2VEC();
        static {
            try {
                w2c.loadJavaModel(DimensionSortMapper.class.getClassLoader().getResource("wiki_vector.mod").getFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void map(ImmutableBytesWritable key, Result value,Context context)
                throws IOException,InterruptedException {
            String name = Bytes.toString(value.getValue(Bytes.toBytes("info"),Bytes.toBytes("name")));
            Text nameText = new Text(name);

            IntWritable sum = new IntWritable(0);
            String keyword = Bytes.toString(value.getValue(Bytes.toBytes("attrs"),Bytes.toBytes("keywords")));
            String [] words = keyword.split(",");
            for(String word:words) {
                if(w2c.getWordVector(word)!=null) {
                    sum.set(sum.get()+1);
                }
            }
            System.out.println(nameText.toString()+" "+sum.get()+" "+keyword);
            context.write(sum,nameText);
        }
    }

    public static class DimensionSortReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        public void reduce(IntWritable key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }

    /**
     *
     * @param args args[0] outputPath
     * @return
     */
    @Override
    public int run(String [] args) throws IOException,InterruptedException,
        ClassNotFoundException{
//        InputSampler.RandomSampler<IntWritable,Text> sampler =
//                new InputSampler.RandomSampler<IntWritable,Text>(0.1,1000);
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf);
        Path outputPath = new Path(args[0]);
        outputPath.getFileSystem(conf).delete(outputPath,true);

        //设置hbase mapper任务
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("zhihu_user",scan,DimensionSortMapper.class,IntWritable.class,
                Text.class,job);
//        job.setReducerClass(DimensionSortReducer.class);
        job.setJarByClass(getClass());
        job.setInputFormatClass(TableInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job,outputPath);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String [] args) {
        try {
            String outputPath = "hdfs://localhost:9000/zhihu_mining/max_dimension";
            int exitCode = ToolRunner.run(new MaxDimension(),new String[] {outputPath});
            System.exit(exitCode);
        }catch (Exception e ) {
            e.printStackTrace();
        }
    }
}
