// Understanding Apache Flink 2. 
// Input:   inputFile.txt
// Output:  outputFile.txt with the values "sensor1" filtered
// Method1: Map + filter, by defining a SingleOutputStreamOperator
// Method2: Map + Filter, by defining to DataStreams "mapped" and "filtered"
//          Uncomment and substitute "filterOut" by "filtered" when executing writeAsCsv
// Params:  inputFile.txt, outputFile.txt


package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Map_filter {

	public static void main(String[] args) {

		// set up the execution environment. DataStream API
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// get input data paths from the 2 args that we have sent by the console (the
		// input and output file)
		String inFilePath = args[0];
		String outFilePath = args[1];
		// Sources are where your program reads its input from
		DataStreamSource<String> source = env.readTextFile(inFilePath);
		@SuppressWarnings("serial")
		SingleOutputStreamOperator<Tuple3<Long, String, Double>> filterOut = source
				.map(new MapFunction<String, Tuple3<Long, String, Double>>() {
					@Override
					public Tuple3<Long, String, Double> map(String in) throws Exception {
						String[] fieldArray = in.split(",");
						Tuple3<Long, String, Double> out = new Tuple3<Long, String, Double>(Long.parseLong(fieldArray[0]), fieldArray[1],
								Double.parseDouble(fieldArray[2]));

						return out;
					}

				}).filter(new FilterFunction<Tuple3<Long, String, Double>>() {
					@Override
					public boolean filter(Tuple3<Long, String, Double> in) throws Exception {
						if (in.f1.equals("sensor1")) {
							return true;
						} else {
							return false;
						}
					}

				});
		
//		DataStream<Tuple3<Long,String,Double>> mapped = source.map(new MapFunction<String,Tuple3<Long,String,Double>>(){
//			@Override
//			public Tuple3<Long,String,Double> map(String in) throws Exception{
//				String[] fieldArray = in.split(",");
//				Tuple3<Long, String, Double> out = new Tuple3<Long, String, Double>(Long.parseLong(fieldArray[0]), fieldArray[1],
//						Double.parseDouble(fieldArray[2]));
//
//				return out;
//			}
//		});
//		
//		DataStream<Tuple3<Long,String,Double>> filtered = mapeo.filter(new FilterFunction<Tuple3<Long,String,Double>>(){
//			@Override
//			public boolean filter(Tuple3<Long,String,Double> in) throws Exception{
//				if (in.f1.equals("sensor1")) {
//					return true;
//				} else {
//					return false;
//				}
//			}
//		});
		
		filterOut.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}