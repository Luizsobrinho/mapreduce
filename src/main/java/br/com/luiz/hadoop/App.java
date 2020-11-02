package br.com.luiz.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exit = ToolRunner.run(new App(), args);
		System.exit(exit);
		;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) {
			System.err.printf("");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(App.class);
		job.setJobName("Contador de Filme");

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		job.setMapperClass(MapperContadorFilmeAno.class);
		job.setReducerClass(ReduceFilmeAno.class);

		System.out.println("Sucesso ?" + job.isSuccessful());
		return returnValue;
	}
}
