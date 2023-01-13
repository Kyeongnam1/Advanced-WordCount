package mju.myhadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Random;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            Configuration conf = context.getConfiguration();
            String parameter = conf.get("parameter");
            if(parameter.equals("upper")) { // 기능: 파라미터를 받아서 모든 문장을 소문자나 대문자로 바꾸기
            	line = value.toString().toUpperCase();
            }else if(parameter.equals("lower")){
            	line = value.toString().toLowerCase();
            }
            StringTokenizer tokenizer = new StringTokenizer(line, "\t\n\r\f,.—`\"()-_:;?![₩~/]' \"\\");
            // 기능: 특수문자 없애기
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable value = new IntWritable(0);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            value.set(sum);
            context.write(key, value);
        }
    }
    
    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(@SuppressWarnings("rawtypes") WritableComparable a, @SuppressWarnings("rawtypes") WritableComparable b) {

          return -super.compare(a, b);

        }

        

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            return -super.compare(b1, s1, l1, b2, s2, l2);

        }

    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

       Path tempDir = new Path("wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));    
       conf.set("parameter", args[2]);
        Job job = new Job(conf, "word count");

        job.setJarByClass(WordCount.class);
        try{
            job.setMapperClass(Map.class);
            job.setCombinerClass(Reduce.class);
            job.setReducerClass(Reduce.class);     
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, tempDir);                                                                             

            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if(job.waitForCompletion(true)) // 기능: 내림차순 sort
            {

                Job sortJob = new Job(conf, "sort");

                sortJob.setJarByClass(WordCount.class);              
                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);

                sortJob.setMapperClass(InverseMapper.class);

                sortJob.setNumReduceTasks(1); 
                FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs[1]));            
                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);

                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        }finally{
            FileSystem.get(conf).deleteOnExit(tempDir);
        }
    }
}
