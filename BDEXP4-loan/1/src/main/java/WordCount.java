
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;


public class WordCount {
    // 日志组名MapCounters，日志名INPUT_WORDS
    static enum MapCounters {
        INPUT_WORDS
    }

    static enum ReduceCounters {
        OUTPUT_WORDS
    }

     static enum CountersEnum { INPUT_WORDS,OUTPUT_WORDS }
//     日志组名CountersEnum，日志名INPUT_WORDS和OUTPUT_WORDS



    /**
     * Reducer没什么特别的升级特性
     *
     * @author Administrator
     */
    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
//            System.out.println(key + ":" +result);
            Counter counter = context.getCounter(
                    ReduceCounters.class.getName(),
                    ReduceCounters.OUTPUT_WORDS.toString());
            counter.increment(1);
        }
    }


    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("ARGS:" + args[0]);
        Path tempDir = new Path("wordcount-temp-output");
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Job solowcjob = Job.getInstance(conf,"solo wordcount");
        solowcjob.setJarByClass(WordCount.class);
        solowcjob.setMapperClass(SoloTokenizerMapper.class);
        solowcjob.setCombinerClass(IntSumReducer.class);
        solowcjob.setReducerClass(IntSumReducer.class);
        solowcjob.setOutputKeyClass(Text.class);
        solowcjob.setOutputValueClass(IntWritable.class);
        solowcjob.setOutputFormatClass(SequenceFileOutputFormat.class);
        List<String> otherArgs = new ArrayList<String>(); // 除了 -skip 以外的其它参数以外的其它参数
        for (int i = 0; i < remainingArgs.length; ++i) {
                otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(solowcjob, new Path(otherArgs.get(0)));// otherArgs的第一个参数是输入路径
        FileOutputFormat.setOutputPath(solowcjob,tempDir);
        if(solowcjob.waitForCompletion(true)) {
            System.out.println("here1");
            Job solosortjob = new Job(conf, "sort");
            solosortjob.setJarByClass(WordCount.class);
            FileInputFormat.addInputPath(solosortjob,tempDir);
            solosortjob.setInputFormatClass(SequenceFileInputFormat.class);
            solosortjob.setMapperClass(InverseMapper.class);
            solosortjob.setReducerClass(SoloSortReducer.class);
            FileOutputFormat.setOutputPath(solosortjob, new Path(otherArgs.get(1)));
            solosortjob.setOutputKeyClass(IntWritable.class);
            solosortjob.setOutputValueClass(Text.class);
            //排序改写成降序
            solosortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            System.out.println("here2");
            System.exit(solosortjob.waitForCompletion(true) ? 0 : 1);
            System.out.println("here3");
            FileSystem.get(conf).delete(tempDir);
            System.exit(0);
        }
    }


    public static class SoloTokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text(); // map输出的key

        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String[] linevalue = line.split(",");
            word.set(linevalue[10]);
            context.write(word, one);
        }
    }

    public static class SoloSortReducer extends
            Reducer<IntWritable,Text,Text,IntWritable> {
        public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            for(Text val: values) {
                context.write(val,key);
            }
        }
    }


}