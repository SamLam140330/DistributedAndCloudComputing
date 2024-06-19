package com.github.samlam140330;

import org.apache.commons.text.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Vector;

public class FriendsOfFriends {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: <in file> [in other files] <out path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Friends of Friends 2023 by SamLam140330");
        job.setJarByClass(FriendsOfFriends.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path output = new Path(otherArgs[otherArgs.length - 1]);
        output.getFileSystem(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FriendMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String one_string = itr.nextToken();
            String two_string = itr.nextToken();
            int one_integer = Integer.parseInt(one_string);
            int two_integer = Integer.parseInt(two_string);

            context.write(new IntWritable(one_integer), new IntWritable(two_integer));
            int mark_red = -1;
            context.write(new IntWritable(two_integer), new IntWritable(one_integer * mark_red));
        }
    }

    public static class FriendReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Vector<Integer> src = new Vector<>();
            Vector<Integer> dest = new Vector<>();

            for (IntWritable value : values) {
                int id = value.get();
                int mark_red = -1;
                if (id / mark_red < 0) {
                    dest.add(id);
                } else {
                    src.add(id / mark_red);
                }
            }

            for (int i = 0; i < src.size(); i++) {
                for (Integer integer : dest) {
                    context.write(new IntWritable(src.elementAt(i)), new IntWritable(integer));
                }
            }
        }
    }
}
