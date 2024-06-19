package com.github.samlam140330;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class q2 {
    public static class VaccinationQ2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String date = line.split(",")[0];
            String ageGroup = line.split(",")[1];

            Date dateConverted;
            Date beforeDate;
            Date afterDate;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            try {
                dateConverted = formatter.parse(date);
                beforeDate = formatter.parse("2023-01-15");
                afterDate = formatter.parse("2021-02-22");
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            if ((dateConverted.after(afterDate) && dateConverted.before(beforeDate)) || dateConverted.equals(beforeDate) || dateConverted.equals(afterDate)) {
                for (int i = 3; i < 9; i++) {
                    if (i == 3) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for Sinovac " + (i - 2) + "st Dose is");
                    } else if (i == 4) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for Sinovac " + (i - 2) + "nd Dose is");
                    } else if (i == 5) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for Sinovac " + (i - 2) + "rd Dose is");
                    } else {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for Sinovac " + (i - 2) + "th Dose is");
                    }
                    outputValue.set(Integer.parseInt(line.split(",")[i]));
                    context.write(outputKey, outputValue);
                }
                for (int j = 9; j < 15; j++) {
                    if (j == 9) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for BioNTech " + (j - 8) + "st Dose is");
                    } else if (j == 10) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for BioNTech " + (j - 8) + "nd Dose is");
                    } else if (j == 11) {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for BioNTech " + (j - 8) + "rd Dose is");
                    } else {
                        outputKey.set("Age from " + ageGroup + " who have vaccines for BioNTech " + (j - 8) + "th Dose is");
                    }
                    outputValue.set(Integer.parseInt(line.split(",")[j]));
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class VaccinationQ2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
