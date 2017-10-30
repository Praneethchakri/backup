package dynamodb.throughPut;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;

public class DynamoAutomaticThroughPut {
	static String ACCESS_KEY;
	static String SECRET_KEY;
	public static DynamoDBMapper dynamoDBMapper = null;
	
	public static List<String> getTablesList(String access,String security){
		List<String> tables = new ArrayList<String>();
	AWSCredentials creds = new BasicAWSCredentials(access,security);
	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
			.withCredentials(new AWSStaticCredentialsProvider(creds))
			.withRegion(Regions.US_EAST_1)
			.build();
	boolean more_tables = true;
    while(more_tables) {
        String last_name = null;
        try {
            ListTablesResult table_list = null;
            if (last_name == null) {
                table_list = client.listTables();
            }

            List<String> table_names = table_list.getTableNames();

            if (table_names.size() > 0) {
                for (String cur_name : table_names) {
//                    System.out.format("* %s\n", cur_name);
                    tables.add(cur_name);
                }
            } else {
                System.out.println("No tables found!");
                System.exit(0);
            }

            last_name = table_list.getLastEvaluatedTableName();
            if (last_name == null) {
                more_tables = false;
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    	}
	return tables;
	}
		@SuppressWarnings("deprecation")
		public static void readucingTableCapacity(String accessKey,String securityKey,List<String>tables){
		AWSCredentials creds = new BasicAWSCredentials(accessKey,securityKey);
		dynamoDBMapper = new DynamoDBMapper(new AmazonDynamoDBClient(creds));
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(creds))
					.withRegion(Regions.US_EAST_1)
					.build();
		DynamoDB dynamodb = new DynamoDB(client);
		Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MINUTE, 3);
			cal.add(Calendar.SECOND, 00);
		Date deadLine = cal.getTime();
		ProvisionedThroughput througPut = new ProvisionedThroughput().withReadCapacityUnits(51L).withWriteCapacityUnits(2L);
		Iterator<String> tableName = tables.iterator();
		while(tableName.hasNext()){
			String table = tableName.next();
		if(!new Date().after(deadLine)){
				UpdateTableRequest updateTableRequest = new UpdateTableRequest(table, througPut);
				UpdateTableResult updateTableResult = client.updateTable(updateTableRequest);
				/*
				 * Here we are reducing the ThroughPut of GSI
				 */
				Table genericTable = dynamodb.getTable(table);
				TableDescription tableDesc = genericTable.describe();
				try{
					if(tableDesc.getGlobalSecondaryIndexes()!=null){
					Iterator<GlobalSecondaryIndexDescription> GSIindexName = tableDesc.getGlobalSecondaryIndexes().iterator();
						if(GSIindexName.next().getIndexName().length()!=0){
							while(GSIindexName.hasNext()){
								GlobalSecondaryIndexDescription gsiDesc = GSIindexName.next();
								String globalIndexName = gsiDesc.getIndexName();
								System.out.println("GlobalIndex Name "+globalIndexName);
								Index index= genericTable.getIndex(globalIndexName);
								  index.updateGSI(througPut.withReadCapacityUnits(51L).withWriteCapacityUnits(2L));
							}
						}
					}else if(tableDesc.getGlobalSecondaryIndexes()==null){
						if(tableName.hasNext()){
							tableName.next();
						}
					}
				}catch(NullPointerException e){
					e.printStackTrace();
					System.out.println("Index Not found..");
				}
				waitForTableToBecomeAvailable(accessKey,securityKey,table);
				}
			else if(new Date().after(deadLine)){
				System.out.println("Completed: " + table);
				ThroughPutTable throughput = dynamoDBMapper.load(ThroughPutTable.class, 1,new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT));
				throughput.setLastEvluatedTable(table);
				dynamoDBMapper.save(throughput, new DynamoDBMapperConfig(SaveBehavior.UPDATE));
				break;
			}		
		}
		}
		@SuppressWarnings("deprecation")
		public static void increaseTableCapacity(String accesKey,String secretKey,List<String> tables){
			AWSCredentials creds = new BasicAWSCredentials(accesKey,secretKey);
			dynamoDBMapper = new DynamoDBMapper(new AmazonDynamoDBClient(creds));
			AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
						.withCredentials(new AWSStaticCredentialsProvider(creds))
						.withRegion(Regions.US_EAST_1)
						.build();
			DynamoDB dynamodb = new DynamoDB(client);
			Calendar cal = Calendar.getInstance();
				cal.add(Calendar.MINUTE, 3);
				cal.add(Calendar.SECOND, 00);
			Date deadLine = cal.getTime();
		String table=null;
			ProvisionedThroughput througPut = new ProvisionedThroughput().withReadCapacityUnits(8L).withWriteCapacityUnits(8L);
			Iterator<String> tableName = tables.iterator();
			while(tableName.hasNext()){
				if(!new Date().after(deadLine)){
					table = tableName.next();
				UpdateTableRequest updateTableRequest = new UpdateTableRequest(table, througPut);
				UpdateTableResult updateTableResult = client.updateTable(updateTableRequest);
				/*
				 * Here we are reducing the ThroughPut of GSI
				 */
				Table genericTable = dynamodb.getTable(table);
				TableDescription tableDesc = genericTable.describe();
				try{
					if(tableDesc.getGlobalSecondaryIndexes()!=null){
					Iterator<GlobalSecondaryIndexDescription> GSIindexName = tableDesc.getGlobalSecondaryIndexes().iterator();
						if(GSIindexName.next().getIndexName().length()!=0){
							while(GSIindexName.hasNext()){
								GlobalSecondaryIndexDescription gsiDesc = GSIindexName.next();
								String globalIndexName = gsiDesc.getIndexName();
								System.out.println("GlobalIndex Name "+globalIndexName);
								Index index= genericTable.getIndex(globalIndexName);
								  index.updateGSI(througPut.withReadCapacityUnits(8L).withWriteCapacityUnits(8L));
							}
						}
					}else if(tableDesc.getGlobalSecondaryIndexes()==null){
						if(tableName.hasNext()){
							tableName.next();
						}
					}
				}catch(NullPointerException e){
					e.printStackTrace();
					System.out.println("Index Not found..");
				}
				waitForTableToBecomeAvailable(accesKey,secretKey,table);
			}else if(new Date().after(deadLine)){
				System.out.println("Completed: " + table);
				ThroughPutTable throughput = dynamoDBMapper.load(ThroughPutTable.class, 1,new DynamoDBMapperConfig(DynamoDBMapperConfig.ConsistentReads.CONSISTENT));
				throughput.setLastEvluatedTable(table);
				dynamoDBMapper.save(throughput, new DynamoDBMapperConfig(SaveBehavior.UPDATE));
				break;
				}		
			}	
		}
		private static void waitForTableToBecomeAvailable(String accessKey,String securityKey,String tableName) {
			AWSCredentials creds = new BasicAWSCredentials(accessKey,securityKey);
			AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
						.withCredentials(new AWSStaticCredentialsProvider(creds))
						.withRegion(Regions.US_EAST_1)
						.build();
			System.out.println("Waiting for " + tableName + " to become ACTIVE...");
			long startTime = System.currentTimeMillis();
			long endTime = startTime + (10 * 60 * 1000);
				System.out.println("StartTime "+startTime);
				System.out.println("EndTime "+endTime);
			while (System.currentTimeMillis() < endTime) {
				DescribeTableRequest request = new DescribeTableRequest()
									.withTableName(tableName);
				TableDescription tableDescription = client.describeTable(
					 request).getTable();
			String tableStatus = tableDescription.getTableStatus();
				System.out.println(" - current state: " + tableStatus);
			if (tableStatus.equals(TableStatus.ACTIVE.toString()))
				return;
			try { Thread.sleep(1000 * 20);
				System.out.println("thead sleep try enterd..");
			} 
			catch (Exception e) { }
				}
			throw new RuntimeException("Table " + tableName + " never went active");
			}
}
