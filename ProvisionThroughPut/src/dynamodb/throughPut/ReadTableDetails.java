package dynamodb.throughPut;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;


public class ReadTableDetails {

	static String ACCESS_KEY;
	static String SECRET_KEY;
	static String method;
	
	public static void main(String[] args) {
		/*Reading configuration file*/
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("config.properties");
			prop.load(input);
			ACCESS_KEY = prop.getProperty("ACCESS_KEY");
			SECRET_KEY = prop.getProperty("SECRET_KEY");
			method = prop.getProperty("METHOD_TYPE");
			
			List<String> tableList = DynamoAutomaticThroughPut.getTablesList(ACCESS_KEY,SECRET_KEY);
			/*String IncreaseCapacity = null;
			String DecreaseCapacity = null;*/
			
			switch (method) {
			case "IncreaseCapacity":
				DynamoAutomaticThroughPut.increaseTableCapacity(ACCESS_KEY,SECRET_KEY,tableList);
				break;
			case "DecreaseCapacity": 
				DynamoAutomaticThroughPut.readucingTableCapacity(ACCESS_KEY,SECRET_KEY,tableList);
				break;
			}
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("Error occurs while reading config.properties file "+e.getMessage());
			return;
		}finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					System.out.println("Error occurs while reading config.properties file "+e.getMessage());
					return;
				}
			}
		}
	}
}
