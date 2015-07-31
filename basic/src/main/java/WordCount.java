import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * A simple word count application
 * using hadoop.
 *
 * Created by babbar on 2015-07-28.
 */
public class WordCount {


    /**
     * Main mapper class used for reading the raw data and creating transformations.
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

            System.out.println(" Going to run the Mapper now.");

//          KEY is LINE NUMBER.
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                outputCollector.collect(word, one);
            }
        }
    }


    /**
     * Instance of the reducer class which takes in the output from the mapper class
     * and perform transformation and reductions on the output.
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

            System.out.println(" Going to execute the Reducer / Combiner");

            int sum = 0;

            while(iterator.hasNext()){
                sum += iterator.next().get();
            }

            outputCollector.collect(text, new IntWritable(sum));
        }
    }



    public static void main(String[] args) throws IOException {

        if(args.length < 2){

            System.err.println("Arguments : i/p file path and o/p file.");
            System.exit(-1);
        }

        JobConf jobConf = new JobConf(WordCount.class);
        jobConf.setJobName("wordcount");

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(Map.class);
        jobConf.setCombinerClass(Reduce.class);
        jobConf.setReducerClass(Reduce.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        JobClient.runJob(jobConf);
    }

}
