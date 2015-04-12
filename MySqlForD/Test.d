module MySqlForD.Test;

unittest{

	import TestingHelper;
	import std.conv;
	import MySqlForD;
	import std.variant;
	import std.stdio;

	YamlFile configurationFile = new YamlFile();
	configurationFile.Open("TestConfig.yaml");

	//connect to the server without database name
	ConnectionParameters noDatabaseConnectionParameters = new ConnectionParameters();
	noDatabaseConnectionParameters.ServerAddress = configurationFile.GetValue("ServerAddress");
	noDatabaseConnectionParameters.Port = to!ushort (configurationFile.GetValue("Port") );
	noDatabaseConnectionParameters.Username = configurationFile.GetValue("Username");
	noDatabaseConnectionParameters.Password = configurationFile.GetValue("Password");
	Connection databaseLessConnection = new Connection(noDatabaseConnectionParameters);
	databaseLessConnection.Connect();
	scope (exit) 
	{
		databaseLessConnection.Disconnect();
	}
	PreparedStatement createDatabaseStatement =  databaseLessConnection.PrepareStatement("CREATE DATABASE test");
	createDatabaseStatement.Execute();

	scope (exit)
	{
		PreparedStatement dropDatabaseStatement =  databaseLessConnection.PrepareStatement("Drop DATABASE test");
		dropDatabaseStatement.Execute();
	}

	//create a database connection to connect to the created database
	ConnectionParameters databaseConnectionParameters = new ConnectionParameters();
	databaseConnectionParameters.ServerAddress = configurationFile.GetValue("ServerAddress");
	databaseConnectionParameters.Port = to!ushort (configurationFile.GetValue("Port") );
	databaseConnectionParameters.Username = configurationFile.GetValue("Username");
	databaseConnectionParameters.Password = configurationFile.GetValue("Password");
	databaseConnectionParameters.DatabaseName = "test";
	Connection testDatabaseConnection = new Connection(databaseConnectionParameters);
	testDatabaseConnection.Connect();

	scope (exit)
	{
		testDatabaseConnection.Disconnect();
	}

	PreparedStatement createTablePreparedStatement = testDatabaseConnection.PrepareStatement("CREATE TABLE User ( Id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,firstname VARCHAR(30) NOT NULL,
																							 lastname VARCHAR(30) NOT NULL, email NVARCHAR(50), score FLOAT );" );
	createTablePreparedStatement.Execute();
	PreparedStatement insertStatement = testDatabaseConnection.PrepareStatement("insert into User (firstname,lastname)values(?,?)");
	
	Variant[] parameters;
	parameters.length =2;
	parameters[0] = "Muhammad";
	parameters[1]="Adel";
	
	insertStatement.Execute(parameters);



}
