package com.github.samlam140330;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Objects;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: <question number> <in file> [in other files] <out path>");
            System.exit(2);
        }

        String questionNumber = "";
        Class<?> jarClass = null;
        Class<? extends Mapper<?, ?, ?, ?>> mapperClass = null;
        Class<? extends Reducer<?, ?, ?, ?>> reducerClass = null;
        Class<?> outputKeyClass = null;
        Class<?> outputValueClass = null;
        if (Objects.equals(otherArgs[0], "1")) {
            questionNumber = "Q1";
            jarClass = q1.class;
            mapperClass = q1.VaccinationQ1Mapper.class;
            reducerClass = q1.VaccinationQ1Reducer.class;
            outputKeyClass = NullWritable.class;
            outputValueClass = Text.class;
        } else if (Objects.equals(otherArgs[0], "2")) {
            questionNumber = "Q2";
            jarClass = q2.class;
            mapperClass = q2.VaccinationQ2Mapper.class;
            reducerClass = q2.VaccinationQ2Reducer.class;
            outputKeyClass = Text.class;
            outputValueClass = IntWritable.class;
        } else if (Objects.equals(otherArgs[0], "3")) {
            questionNumber = "Q3";
            jarClass = q3.class;
            mapperClass = q3.VaccinationQ3Mapper.class;
            reducerClass = q3.VaccinationQ3Reducer.class;
            outputKeyClass = Text.class;
            outputValueClass = IntWritable.class;
        } else if (Objects.equals(otherArgs[0], "4")) {
            questionNumber = "Q4";
            jarClass = q4.class;
            mapperClass = q4.VaccinationQ4Mapper.class;
            reducerClass = q4.VaccinationQ4Reducer.class;
            outputKeyClass = Text.class;
            outputValueClass = IntWritable.class;
        } else if (Objects.equals(otherArgs[0], "5")) {
            questionNumber = "Q5";
            jarClass = q5.class;
            mapperClass = q5.VaccinationQ5Mapper.class;
            reducerClass = q5.VaccinationQ5Reducer.class;
            outputKeyClass = Text.class;
            outputValueClass = IntWritable.class;
        }

        if (questionNumber.isEmpty()) {
            System.err.println("Invalid question number");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Vaccination " + questionNumber + " 2023 by SamLam140330");
        job.setJarByClass(jarClass);
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        for (int i = 1; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path output = new Path(otherArgs[otherArgs.length - 1]);
        output.getFileSystem(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
