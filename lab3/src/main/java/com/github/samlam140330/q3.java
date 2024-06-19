package com.github.samlam140330;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class q3 {
    public static class VaccinationQ3Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String date = line.split(",")[0];
            String yearMonth = date.split("-")[0] + "-" + date.split("-")[1];
            int[] sinovacDose = new int[6];
            int[] bioNTechDose = new int[6];

            for (int i = 3; i < 15; i++) {
                if (i < 9) {
                    sinovacDose[i - 3] = Integer.parseInt(line.split(",")[i]);
                } else {
                    bioNTechDose[i - 9] = Integer.parseInt(line.split(",")[i]);
                }
            }

            switch (yearMonth) {
                case "2021-12":
                case "2022-01":
                case "2022-02":
                case "2022-03":
                    for (int i : sinovacDose) {
                        outputKey.set("The total number of people who received Sinovac in " + yearMonth + " is ");
                        outputValue.set(i);
                        context.write(outputKey, outputValue);
                    }
                    for (int i : bioNTechDose) {
                        outputKey.set("The total number of people who received BioNTech in " + yearMonth + " is ");
                        outputValue.set(i);
                        context.write(outputKey, outputValue);
                    }
                    break;
                default:
                    break;
            }
        }
    }

    public static class VaccinationQ3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total += value.get();
            }
            outputValue.set(total);
            context.write(key, outputValue);
        }
    }
}
