
/** 2.
package com.mapreduce.mm;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {
    // Mapper Class
    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String matrixName = tokens[0]; // M or N
            int i = Integer.parseInt(tokens[1]);
            int j = Integer.parseInt(tokens[2]);
            float val = Float.parseFloat(tokens[3]);
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n")); // Matrix dimension
            if (matrixName.equals("M")) {

                // Emit for each column in N
                for (int k = 0; k < n; k++) {
                    context.write(new Text(i + "," + k), new Text("M," + j + "," + val));
                }
            } else

            {
                // Emit for each row in M
                for (int k = 0; k < n; k++) {
                    context.write(new Text(k + "," + j), new Text("N," + i + "," + val));
                }
            }
        }
    }

    // Reducer Class
    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<Integer, Float> hashA = new HashMap<>();
            HashMap<Integer, Float> hashB = new HashMap<>();
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String matrix = parts[0];
                int index = Integer.parseInt(parts[1]);
                float value = Float.parseFloat(parts[2]);
                if (matrix.equals("M")) {
                    hashA.put(index, value);
                } else {

                    hashB.put(index, value);
                }
            }

            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));
            float result = 0.0f;
            for (int k = 0; k < n; k++) {
                float a = hashA.getOrDefault(k, 0.0f);
                float b = hashB.getOrDefault(k, 0.0f);
                result += a * b;
            }
            if (result != 0.0f) {
                context.write(key, new Text(String.valueOf(result)));
            }
        }
    }

    // Driver (Main Method)
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Set matrix dimension n (number of columns in M or rows in N)
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");

        conf.set("n", "3");

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
*/

/** 3. 
package com.mapreduce.we;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {
    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (fields.length == 4) {
                String date = fields[0];
                try {
                    float tmax = Float.parseFloat(fields[1]);
                    float tmin = Float.parseFloat(fields[2]);
                    float prcp = Float.parseFloat(fields[3]);
                    String message = "";
                    if (tmax > 35) {
                        message += "Hot day ";
                    }
                    if (tmin < 15) {
                        message += "Cold day ";
                    }
                    if (prcp > 10) {
                        message += "Rainy day ";
                    }
                    if (message.isEmpty()) {
                        message = "Normal weather";
                    }
                    context.write(new Text(date), new Text(message.trim()));
                } catch (

                NumberFormatException e) {
                    // Ignore malformed rows
                }
            }
        }
    }

    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val); // One value per date
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("n", "3");
        Job job = Job.getInstance(conf, "Weather Analysis");
        job.setJarByClass(WeatherAnalysis.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path
    }
}
*/

/** 4. 
package com.mapreduce.tg;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieTagsWithTitles {
    public static class TagsMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",", 4); // userId, movieId, tag, timestamp
            if (fields.length >= 3 && !fields[0].equals("userId")) { // skip header
                String movieId = fields[1].trim();
                String tag = fields[2].trim();
                context.write(new Text(movieId), new Text(tag));
            }
        }
    }

    public static class TagsReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text movieId, Iterable<Text> tags, Context context)
                throws IOException, InterruptedException {
            HashSet<String> uniqueTags = new HashSet<>();
            for (Text tag : tags) {
                uniqueTags.add(tag.toString());
            }
            context.write(movieId, new Text(String.join(", ", uniqueTags)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("n", "3");

        Job job = Job.getInstance(conf, "Movie Tags By Movie ID");
        job.setJarByClass(MovieTagsWithTitles.class);
        job.setMapperClass(TagsMapper.class);
        job.setReducerClass(TagsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Input/output from command-line args
        FileInputFormat.addInputPath(job, new Path(args[0])); // path to tags.csv
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output folder
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

*/

/** 8. 
package com.mapreduce.wc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // Ensure correct input arguments
        if (files.length < 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = Job.getInstance(c, "wordcount");
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // Mapper Class
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordText = new Text();

        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split("\\s+"); // Handles multiple spaces
            for (String word : words) {
                if (!word.isEmpty()) { // Avoid empty strings
                    wordText.set(word.trim().toUpperCase());
                    con.write(wordText, one);
                }
            }
        }
    }

    // Reducer Class
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }
}
*/