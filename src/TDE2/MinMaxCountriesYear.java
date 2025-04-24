package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import java.text.DecimalFormat;

public class MinMaxCountriesYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/Question8TDE.txt");

        Job job = Job.getInstance(conf, "MaxMinTransactionByCountryYear");

        job.setJarByClass(MinMaxCountriesYear.class);

        job.setMapperClass(MinMaxTransactionMapper.class);
        job.setMapOutputKeyClass(CountryYearKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(MinMaxTransactionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Register input and output files
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        if (job.waitForCompletion(true)) {
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);

            Path outputFile = new Path("output/Question8TDETEXT.txt");
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

    public static class CountryYearKey implements WritableComparable<CountryYearKey> {
        private String country;
        private String year;

        public CountryYearKey() {
            this.country = "";
            this.year = "";
        }

        public CountryYearKey(String country, String year) {
            this.country = country;
            this.year = year;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(country);
            out.writeUTF(year);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            country = in.readUTF();
            year = in.readUTF();
        }

        @Override
        public int compareTo(CountryYearKey other) {
            int countryCompare = this.country.compareTo(other.country);
            if (countryCompare != 0) {
                return countryCompare;
            }
            return this.year.compareTo(other.year);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CountryYearKey) {
                CountryYearKey other = (CountryYearKey) obj;
                return this.country.equals(other.country) && this.year.equals(other.year);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return country.hashCode() * 163 + year.hashCode();
        }

        @Override
        public String toString() {
            return country + "\t" + year;
        }
    }

    public static class MinMaxTransactionMapper extends Mapper<LongWritable, Text, CountryYearKey, DoubleWritable> {
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
                try {
                    String country = fields[0];
                    String year = fields[1];

                    double price = Double.parseDouble(fields[5]);
                    double amount = Double.parseDouble(fields[8]);
                    double finalprice = price * amount;
                    System.err.println(String.valueOf(finalprice));
                    CountryYearKey countryYearKey = new CountryYearKey(country, year);
                    context.write(countryYearKey, new DoubleWritable(amount));
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    context.getCounter("MaxMinTransaction", "InvalidRecord").increment(1);
                }
            }
        }
    }

    public static class MinMaxTransactionReducer extends Reducer<CountryYearKey, DoubleWritable, Text, Text> {
        private final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0");

        @Override
        public void reduce(CountryYearKey key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (DoubleWritable value : values) {
                double currentValue = value.get();
                min = Math.min(min, currentValue);
                max = Math.max(max, currentValue);
            }

            String minFormatted = DECIMAL_FORMAT.format(min);
            String maxFormatted = DECIMAL_FORMAT.format(max);

            Text outKey = new Text(key.getCountry());

            context.write(outKey, new Text(key.getYear() + "\tmin\t" + minFormatted));

            context.write(outKey, new Text(key.getYear() + "\tmax\t" + maxFormatted));
        }
    }
}