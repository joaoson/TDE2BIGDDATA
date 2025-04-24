package TDE2;

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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class CountTransactionBrazil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Input and output paths
        //Path input = new Path(args[0]);
        //Path output = new Path(args[1]);

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Question1TDE.txt");

        Job job = Job.getInstance(conf, "TransactionBrazil");

        job.setJarByClass(CountTransactionBrazil.class);
        job.setMapperClass(CountTransactionYearMapper.class);
        job.setReducerClass(CountTransactionYearReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);

            Path outputFile = new Path("output/Question1TDETEXT.txt");
            org.apache.hadoop.fs.FSDataOutputStream out = fs.create(outputFile);

            org.apache.hadoop.fs.FileStatus[] status = fs.listStatus(output,
                    new org.apache.hadoop.fs.PathFilter() {
                        public boolean accept(Path path) {
                            return path.getName().startsWith("part-");
                        }
                    });

            for (org.apache.hadoop.fs.FileStatus fileStatus : status) {
                org.apache.hadoop.fs.FSDataInputStream in = fs.open(fileStatus.getPath());
                org.apache.hadoop.io.IOUtils.copyBytes(in, out, conf, false);
                in.close();
            }

            out.close();
            System.exit(0);
        } else {
            System.exit(1);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class CountTransactionYearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("country_or_area;year;comm_code")) {
                return;
            }

            String[] fields = line.split(";");
            if(fields[9].equals("all_commodities") || fields[8].equals("0") || fields[8].isEmpty()  || fields[6].isEmpty()){
                System.err.println(line);
                return;
            }

            if (
                    !fields[0].equalsIgnoreCase("Brazil")
            ) {
                return;
            }

            if (fields.length <11) {
                word.set(fields[0]);
                context.write(word, one);
            }
        }
    }

    public static class CountTransactionYearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}