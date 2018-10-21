// Understanding Apache Flink 1. 

// Input:  inputFile.txt
// Output: outputFile.txt with the values of each line between ()
// Method: Map
// Params: inputFile.txt, outputFile.txt


package org.apache.flink;

import org.apache.flink.core.fs.FileSystem;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceSink {

	public static void main(String[] args) {
		
		// Set up the execution environment. DataStream API
		final StreamExecutionEnvironment env = 
		StreamExecutionEnvironment.getExecutionEnvironment();
		
		// Get input data paths from the 2 args that we have sent by the console (the input and output file)
		String inFilePath = args[0];
		String outFilePath = args[1];
		
		// Sources are where your program reads its input from
		DataStreamSource<String> source = env.readTextFile(inFilePath);
		
		// SingleOutputStreamOperator represents a user defined transformation applied on a DataStream with one predefined output type.
		// In this case, my defined transformation is called: map , and the output type is a Tuple3<Long, String, Double>
		
		// In this case, we apply to our DataStream, the predefined transformation Map: Takes one element and produces one element
		// The transformation calls a MapFunction for each element of the DataStream.
		
		// We have to pass the parameter mapper - The MapFunction that is called for each element of the DataStream. 
		// We redefine the map function in the moment.
		
		@SuppressWarnings("serial")
		SingleOutputStreamOperator<Tuple3<Long, String, Double>> map = source.map(new MapFunction<String, Tuple3<Long, String, Double>>() {
			@Override
			public Tuple3<Long, String, Double> map(String value) throws Exception {
				
				String[] arrayv = value.split(",");
				
				Tuple3<Long, String, Double> out = 
						new Tuple3<Long, String, Double>(Long.parseLong(arrayv[0])
						, arrayv[1], Double.parseDouble(arrayv[2]));
				
				return out;
			}
		});
		
		
		
		map.writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE);
		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}