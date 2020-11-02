package br.com.luiz.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceFilmeAno extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text chave, Iterable<IntWritable> valores, Context context)
			throws IOException, InterruptedException {

		int somatorio = 0;
		Iterator<IntWritable> valueIterator = valores.iterator();
		while (valueIterator.hasNext()) {
			somatorio += valueIterator.next().get();

		}
		context.write(chave, new IntWritable(somatorio));
	}
}
