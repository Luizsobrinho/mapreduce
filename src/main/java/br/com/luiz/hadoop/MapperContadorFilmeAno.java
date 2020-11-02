package br.com.luiz.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperContadorFilmeAno extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String linha = value.toString();

		StringTokenizer token = new StringTokenizer(linha, "::");
		String filmeId = token.nextToken();
		String titulo = token.nextToken();

		if ((!titulo.isEmpty()) && (titulo.length() >= 6)) {
			String ano = titulo.substring(titulo.length() - 5, titulo.length() - 1);
			try {
				context.write(new Text(ano), ONE);
			} catch (Exception e) {
				System.out.println("Erro ao mapear: " + titulo + " " + filmeId);
			}
		}

	}
}
