package test;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.*;
import java.net.URI;
import java.util.*;

public class IndustryCount {
    public static class ICMap extends Mapper<LongWritable, Text, Text, LongWritable>{

        private boolean first = true;

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context){
            if(first){
                first = false;
                return;
            }
            String train_data = value.toString();
            String[] features = train_data.split(",");
            String industry = features[10];
//            System.out.println(industry);
            try {
                context.write(new Text(industry), new LongWritable(1));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class ICReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                              Context context
        ){
            long count = 0;
            for(LongWritable value: values){
                count += value.get();
            }
            try {
                context.write(key, new LongWritable(count));
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static class TextIntWritable implements WritableComparable<TextIntWritable>{

        Text word;
        IntWritable count;
        public TextIntWritable(){
            set(new Text(), new IntWritable());
        }
        public void set(Text word, IntWritable count){
            this.word = word;
            this.count = count;
        }
        public void write(DataOutput out) throws IOException {
            word.write(out);
            count.write(out);
        }
        public void readFields(DataInput in) throws IOException {
            word.readFields(in);
            count.readFields(in);
        }
        @Override
        public String toString(){
            return word.toString() + " " + count.toString();
        }
        @Override
        public int hashCode(){
            return this.word.hashCode() + this.count.hashCode();
        }
        public int compareTo(TextIntWritable o) {
            int result = -1 * this.count.compareTo(o.count);  //先比较次数
            if(result != 0)
                return result;
            return this.word .compareTo(o.word); //次数相同，则按字典排序
        }
    }

    public static class ISMapper extends Mapper<LongWritable, Text , TextIntWritable, NullWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context){
            try{
                TextIntWritable k = new TextIntWritable();
                String []string = value.toString().split("\t");
                k.set(new Text(string[0]), new IntWritable(Integer.valueOf(string[1])));
                context.write(k,  NullWritable.get());
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ISReducer extends Reducer<TextIntWritable, NullWritable, TextIntWritable, NullWritable>{
        public void reduce(TextIntWritable key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
            for(NullWritable v : value)
                context.write(key, v);
        }
    }

    public static boolean run(String in, String tmp, String out){
        Configuration conf = new Configuration();
        try {
            Job job = new Job(conf, "Industry Count");
            System.out.println("Begin job: Industry Count");
            job.setJarByClass(IndustryCount.class);
            job.setMapperClass(ICMap.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setReducerClass(ICReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(tmp));
            if(job.waitForCompletion(true)){
                Job sortJob = new Job(conf, "Industry Sort");
                System.out.println("Begin job: Industry Sort");
                sortJob.setJarByClass(IndustryCount.class);

                sortJob.setMapperClass(ISMapper.class);
                sortJob.setReducerClass(ISReducer.class);
                sortJob.setOutputKeyClass(TextIntWritable.class);
                sortJob.setOutputValueClass(NullWritable.class);
                FileInputFormat.setInputPaths(sortJob, new Path(tmp));
                FileOutputFormat.setOutputPath(sortJob, new Path(out));
                return sortJob.waitForCompletion(true);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
}
