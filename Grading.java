import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Grading {
	public static void main(String[] argv) {

		int caseId = Integer.parseInt(argv[0]);
		int nodeNum = 5;
		int[] result = {0, 0, 0, 0, 0};
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));

			String input;
			int nodeId = 0;
			int oldest = 0;

			while ((input = br.readLine()) != null) {
				//System.out.println(input + " XXX");
				String parts[] = input.split("\\s+");
				nodeId = (int)Double.parseDouble(parts[0]);
				oldest = (int)Double.parseDouble(parts[1]);
				
				//System.out.println(nodeId + ": " + oldest);
				result[nodeId] = oldest;
			}

		} catch (IOException io) {
			io.printStackTrace();
		}
		
		// Test the result
		int score = 0;
		int correctNum = 0;
		int[] groundTruth = {0, 0, 0, 0, 0};
		switch(caseId) {
		case 1:
			groundTruth[0] = 66;
			groundTruth[1] = 66;
			groundTruth[2] = -1;
			groundTruth[3] = 49;
			groundTruth[4] = 66;
			break;
		case 2:
			groundTruth[0] = 40;
			groundTruth[1] = 40;
			groundTruth[2] = 55;
			groundTruth[3] = 55;
			groundTruth[4] = 40;			
			break;
		}
		
		System.out.println("Test result for case " + caseId + ":");
		System.out.println("-------------------------------------");

		for (int i = 0; i < nodeNum; ++i) {
			if (result[i] == groundTruth[i]) {
				System.out.println("Node " + i + " passed");	
				correctNum++;
			} else {
				System.out.println("Node " + i + " failed, it should have oldest follower with age " + groundTruth[i] + ", but the program outputs " + result[i]);
			}
		}
		score = (int)(((double)correctNum/(double)nodeNum)*50);
		
		System.out.println("You scored " + score + "/50 for test case " + caseId);
		
	}
}
