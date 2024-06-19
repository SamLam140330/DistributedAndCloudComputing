package com.github.samlam140330;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q5 {
    public static class VaccinationQ5Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String date = line.split(",")[0];
            String yearMonth = date.split("-")[0] + "-" + date.split("-")[1];
            int totalVaccinations = 0;

            for (int i = 3; i < 15; i++) {
                totalVaccinations += Integer.parseInt(line.split(",")[i]);
            }

            outputKey.set("Total number of vaccinations between two consecutive month in " + yearMonth + " is: ");
            outputValue.set(totalVaccinations);
            context.write(outputKey, outputValue);
        }
    }

    public static class VaccinationQ5Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outputValue = new IntWritable();
        private int previousTotal = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int currentTotal = 0;
            int difference;
            for (IntWritable value : values) {
                currentTotal += value.get();
            }

            if (previousTotal != 0) {
                if (previousTotal > currentTotal) {
                    difference = previousTotal - currentTotal;
                } else {
                    difference = currentTotal - previousTotal;
                }
                outputValue.set(difference);
                context.write(key, outputValue);
            }
            previousTotal = currentTotal;
        }
    }
}
