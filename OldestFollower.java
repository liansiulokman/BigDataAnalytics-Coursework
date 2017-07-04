import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.BasicComputation;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

/**
	Vertex ID: LongWritable
	Vertex value: DoubleWritable
	Edge value: FloatWritable
	Message: DoubleWritable
*/

public class OldestFollower extends BasicComputation<LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	
	@Override
	public void compute(Vertex<LongWritable, DoubleWritable, FloatWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
			
			
			if(getSuperstep()==0){
				sendMessageToAllEdges(vertex, vertex.getValue());
				vertex.setValue(new DoubleWritable(-1));
			}
			else{
				boolean change = false;
				Double max = 0.0;
				for(DoubleWritable msg:messages){
					if(msg.get()>max){
						max = msg.get();
						vertex.setValue(new DoubleWritable(max));
						change = true;
					}
				}
			}
			// This is a FAKE implementation
			// Please replace it with your own implementation
			vertex.voteToHalt();
			
	}	
}