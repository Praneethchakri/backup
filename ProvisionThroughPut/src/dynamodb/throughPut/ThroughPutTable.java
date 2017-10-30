package dynamodb.throughPut;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="ThroughPutTable")
public class ThroughPutTable {
	
	private String lastEvluatedTable;
	public int id;
	@DynamoDBAttribute(attributeName="lastEvluatedTable")
	public String getLastEvluatedTable() {
		return lastEvluatedTable;
	}
	
	public void setLastEvluatedTable(String lastEvluatedTable) {
		this.lastEvluatedTable = lastEvluatedTable;
	}
	@DynamoDBHashKey(attributeName="id")
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
}
