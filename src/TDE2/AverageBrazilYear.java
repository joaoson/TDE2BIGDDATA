package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AverageBrazilYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Question5TDE.txt");
        Job job = Job.getInstance(conf, "AverageBrazil");

        job.setJarByClass(AverageBrazilYear.class);

        job.setMapperClass(AverageBrazilMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueCountWritable.class);

        job.setCombinerClass(AverageBrazilCombiner.class);

        job.setReducerClass(AverageBrazilReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);

            Path outputFile = new Path("output/Question5TDETEXT.txt");
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

    public static class ValueCountWritable implements Writable {
        private LongWritable value;
        private LongWritable count;

        public ValueCountWritable() {
            this.value = new LongWritable();
            this.count = new LongWritable();
        }

        public ValueCountWritable(long value, long count) {
            this.value = new LongWritable(value);
            this.count = new LongWritable(count);
        }

        public LongWritable getValue() {
            return value;
        }

        public LongWritable getCount() {
            return count;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            value.write(out);
            count.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            value.readFields(in);
            count.readFields(in);
        }
    }

    public static class AverageBrazilMapper extends Mapper<LongWritable, Text, Text, ValueCountWritable> {
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
                    fields[0].equalsIgnoreCase("Other Asia, nes") ||
                            fields[0].equalsIgnoreCase("Belgium-Luxembourg") ||
                            fields[0].equalsIgnoreCase("EU-28")
            ) {
                return;
            }

            if (fields.length <11) {
                if (fields[0].equals("Brazil")) {
                    try {
                        word.set(fields[1]);
                        String valueStr = fields[5].trim();
                        long numericValue = Long.parseLong(valueStr);
                        if (numericValue > 0) {
                            context.write(word, new ValueCountWritable(numericValue, 1));
                        }
                    } catch (NumberFormatException e) {
                        return;
                    }
                }
            }
        }
    }

    public static class AverageBrazilCombiner extends Reducer<Text, ValueCountWritable, Text, ValueCountWritable> {
        @Override
        public void reduce(Text key, Iterable<ValueCountWritable> values, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            long count = 0;

            for (ValueCountWritable vc : values) {
                total += vc.getValue().get();
                count += vc.getCount().get();
            }

            context.write(key, new ValueCountWritable(total, count));
        }
    }

    public static class AverageBrazilReducer extends Reducer<Text, ValueCountWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<ValueCountWritable> values, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            long count = 0;

            for (ValueCountWritable vc : values) {
                total += vc.getValue().get();
                count += vc.getCount().get();
            }

            if (count != 0) {
                long average = total / count;

                String result = "Final totals - sum: " + total +
                        ", count: " + count +
                        ", average: " + average;

                System.err.println(result);

                context.write(key, new IntWritable((int) average));
            }
        }
    }
}