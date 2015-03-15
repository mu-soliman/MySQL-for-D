module DatabaseHandler;
import std.socket;
import std.bitmanip;
import std.system;
import std.stdio;
import std.digest.sha;
import Common.Functions;
import Common.Exceptions;
import std.variant;

/***********************************
This is a connection that should be pooled later (didn't write pooling code yet). This is why I had to create a connection object separate from the database handler object. This pooling should be transparent to the database
handler user, this is why it is private class
*/
private class Connection {

	static const uint AUTHENTICATION_PLUGIN_DATA_PART1_LENGTH			=	8;
	static const uint RESERVED_SERVER_STRING_LENGTH						=	10;
	static const uint SERVER_STATUS_LENGTH								=	2;

	static const uint RESERVED_CLIENT_STRING_LENGTH						=	23;

	private ConnectionParameters	_ConnectionParameters;
	private uint					_ProtocolVersion;
	private Socket					_Socket;
	private string					_ServerVersion;
	private uint					_ConnectionId;
	private string					_AuthenticationPluginName;
	private ubyte					_ServerCharacterSet;
	private uint					_ServerCapabilities;
	private ubyte[2]				_ServerStatus;
	private ubyte[]					_ServerAuthenticationPluginData;

	/*********************************************************************
	Buffer used for differnt tasks to avoid successive allocation and deallocation
	*/
	private ubyte[]					_TempBuffer;

	private enum CapabilityFlags
	{
		CLIENT_PROTOCOL_41			=	0x00000200,
		CLIENT_LONG_PASSWORD		=	0x00000001,
		CLIENT_CONNECT_WITH_DB		=   0x00000008,
		CLIENT_SECURE_CONNECTION	=   0x00008000 
	}
	private enum PreparedStatementCommands
	{
		COM_STMT_PREPARE = 22,
		COM_STMT_EXECUTE = 23,
		COM_STMT_SEND_LONG_DATA = 24,
		COM_STMT_CLOSE = 25

	}

	private @property uint ProtocolVersion()
	{
		return _ProtocolVersion;
	}
	
	private @property ServerVersion ()
	{
		return _ServerVersion;
	}
	
	private this()
	{
		_Socket = new TcpSocket();
		_TempBuffer.length = 256;
	}

	private void Connect(ConnectionParameters parameters)
	{
		_ConnectionParameters = parameters;
		_Socket.connect(new InternetAddress(parameters.ServerAddress,parameters.Port));
		
		//create an alias for _TempBuffer to slice easily without _TempBuffer gets affected
		ubyte[] buffer = _TempBuffer;
		
		_Socket.receive(buffer);

		//packet size is found in the first three bytes
		ubyte[4] packetSizeBytes;
		packetSizeBytes[0..3]= buffer[0..3];
		//add a forth byte to be able to convert to a uint
		packetSizeBytes[3]=0;
		
		uint packetSize = peek! (uint,Endian.littleEndian)(cast (ubyte[]) packetSizeBytes);
		//remove bytes we have already consumed
		buffer = buffer[3..$];
		
		uint packetSequenceNumber = cast (uint) buffer[0];
		//once again remove bytes we have consumed
		buffer = buffer[1..$];

		//remove useless bytes
		buffer = buffer[0..packetSize-1];
		

		ushort initial_byte = buffer[0];

		if (initial_byte == 0xFF)
			ProcessErrorMessage(buffer);
		else
		{
			ProcessInitialHandshakeMessage(buffer);
			SendClientHandshakeResponseMessage();
			HandleServerHandshakeResponse();
		}

	}
	
	private void ProcessErrorMessage (ref ubyte[] error)
	{

	}
	
	private void ProcessInitialHandshakeMessage(ref ubyte[] initialHandshakeMessage)
	{
		_ProtocolVersion = initialHandshakeMessage[0];
		//remove bytes we have consumed
		initialHandshakeMessage = initialHandshakeMessage[1..$];

		_ServerVersion = ReadString(initialHandshakeMessage);
		_ConnectionId = read! (uint,Endian.littleEndian)(initialHandshakeMessage);

		ubyte[] authPluginDataPart1 = initialHandshakeMessage[0..AUTHENTICATION_PLUGIN_DATA_PART1_LENGTH];
		//remove bytes we have consumed
		initialHandshakeMessage = initialHandshakeMessage[AUTHENTICATION_PLUGIN_DATA_PART1_LENGTH..$];
		//remove filler byte
		initialHandshakeMessage = initialHandshakeMessage[1..$];
		
		ubyte[4] serverCapabilitiesBytes;
		//server capabilities lower bytes
		serverCapabilitiesBytes[0..2]= initialHandshakeMessage[0..2];
		initialHandshakeMessage = initialHandshakeMessage[2..$];

		_ServerCharacterSet = initialHandshakeMessage[0];
		initialHandshakeMessage = initialHandshakeMessage[1..$];

		_ServerStatus = initialHandshakeMessage[0..SERVER_STATUS_LENGTH];
		initialHandshakeMessage = initialHandshakeMessage[SERVER_STATUS_LENGTH..$];
		
		//server capabilities uper bytes
		serverCapabilitiesBytes[2..4] = initialHandshakeMessage[0..2];
		initialHandshakeMessage = initialHandshakeMessage[2..$];
		_ServerCapabilities = peek!(uint,Endian.littleEndian)(cast (ubyte[]) serverCapabilitiesBytes);


		uint totalLentgthOfAuthPluginData = initialHandshakeMessage[0];
		//remove bytes we have consumed
		initialHandshakeMessage = initialHandshakeMessage[1..$];

		//skip empty reserved string
		initialHandshakeMessage = initialHandshakeMessage[RESERVED_SERVER_STRING_LENGTH .. $];
		
		uint lengthOfAuthPluginDataPart2 = totalLentgthOfAuthPluginData -8;
		ubyte[]authPluginDataPart2=  initialHandshakeMessage[0 .. lengthOfAuthPluginDataPart2];
		

		//remove bytes we consumed
		initialHandshakeMessage = initialHandshakeMessage[lengthOfAuthPluginDataPart2..$];

		_AuthenticationPluginName = ReadString(initialHandshakeMessage); 

		assert (_AuthenticationPluginName == "mysql_native_password");
		assert (authPluginDataPart2[$-1] == '\0');
		assert (authPluginDataPart2.length == 13);

		_ServerAuthenticationPluginData.length = 20;
		_ServerAuthenticationPluginData[0..8]=authPluginDataPart1;
		_ServerAuthenticationPluginData[8..$]=authPluginDataPart2[0..12];
	}

	private void SendClientHandshakeResponseMessage()
	{
		//create an alias for _TempBuffer to slice easily without _TempBuffer gets affected
		ubyte[] handshakeResponseMessage = _TempBuffer;
		
		//first 4 bytes are for packet header that we will write at the end of this method
		uint currentIndex =4;

		uint capabilities =GenerateCapabilityFlags();
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,capabilities,currentIndex);
		currentIndex += 4;

		//maximum size for a command that we may send to the database.we put no resteriction from our side
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,0,currentIndex);
		currentIndex +=4;

		//use the default character set for mysql which is latin1_swedish_ci 
		write!(ubyte,Endian.littleEndian)(handshakeResponseMessage, 0x08 ,currentIndex);
		currentIndex ++;

		uint endOfReservedClientStringIndex = currentIndex + RESERVED_CLIENT_STRING_LENGTH;
		//reserved client string, all set to zero
		for(;currentIndex < endOfReservedClientStringIndex;currentIndex++)
		{
			handshakeResponseMessage[currentIndex]=0;
		}

		//strings in D are not null terminated and the protocol expected a null terminated string for the username
		string userName = _ConnectionParameters.Username ~ "\0";
		WriteString(handshakeResponseMessage,userName,currentIndex);

		ubyte[] authenticationResponse =  GenerateAuthenticationResponse();
		handshakeResponseMessage[currentIndex]= cast(ubyte) authenticationResponse.length;
		currentIndex++;
		handshakeResponseMessage[currentIndex..currentIndex+authenticationResponse.length] = authenticationResponse;
		currentIndex +=authenticationResponse.length;

		//authentication response is null terminated
		WriteString(handshakeResponseMessage,"\0",currentIndex);

		uint x = _ServerCapabilities & CapabilityFlags.CLIENT_CONNECT_WITH_DB;
		
		if (_ConnectionParameters.DatabaseName.length >0 && (_ServerCapabilities & CapabilityFlags.CLIENT_CONNECT_WITH_DB))
		{
			string databaseName = _ConnectionParameters.DatabaseName ~ "\0";
			WriteString(handshakeResponseMessage,databaseName,currentIndex);
		}


		//packet length exclues the 4 bytes packet header
		uint packetLength = currentIndex -4;
		/*write the packet length in the first 4 bytes (packet header). The protocol specifies that only 3 bytes are for the packet length and the forth is for the packet sequence. 
		We will overwrite the forth byte later*/
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,packetLength,0);
		//write the packet sequeence in the forth byte
		handshakeResponseMessage[3]=1;
		
		 _Socket.send(handshakeResponseMessage[0..currentIndex]);

	}
	
	private uint GenerateCapabilityFlags()
	{
		uint capabilities =0;
		capabilities = capabilities | CapabilityFlags.CLIENT_PROTOCOL_41;
		capabilities = capabilities | CapabilityFlags.CLIENT_LONG_PASSWORD;
		if ( _ConnectionParameters.DatabaseName.length > 0)
			capabilities = capabilities | CapabilityFlags.CLIENT_CONNECT_WITH_DB;
		capabilities = capabilities | CapabilityFlags.CLIENT_SECURE_CONNECTION;

		return capabilities;
	}
	
	private ubyte[] GenerateAuthenticationResponse()
	{
		ubyte[] hashedPassword = sha1Of(_ConnectionParameters.Password);
		writeln(_ConnectionParameters.Password);
		ubyte[] hashOfHashedPassword = sha1Of(hashedPassword);
		ubyte[40] concatenatedArray;
		concatenatedArray[0..20] = _ServerAuthenticationPluginData;
		concatenatedArray[20..$] = hashOfHashedPassword;
		ubyte[] concatenatedHash = sha1Of(concatenatedArray);
		ubyte[] authenticationResponse;
		authenticationResponse.length = 20;
		for(int i=0;i<hashedPassword.length;i++)
		{
			authenticationResponse[i]=hashedPassword[i] ^ concatenatedHash[i];
		}
		return authenticationResponse;
	}
	
	private void HandleServerHandshakeResponse()
	{
		//create an alias for _TempBuffer to slice easily without _TempBuffer gets affected
		ubyte[] buffer = _TempBuffer;
		_Socket.receive(buffer);
		//packet size is found in the first three bytes
		ubyte[4] packetSizeBytes;
		packetSizeBytes[0..3]= buffer[0..3];
		//add a forth byte to be able to convert to a uint
		packetSizeBytes[3]=0;
		uint packetSize = peek! (uint,Endian.littleEndian)(cast (ubyte[]) packetSizeBytes);

		//remove packet header after reading it
		buffer = buffer[3..$];
		uint packetSequenceNumber = cast (uint) buffer[0];
		buffer = buffer[1..$];

		//remove useless bytes
		buffer = buffer[0..packetSize];
		
		if (buffer[0]==0xff)
		{
			HandleErrorPacket(buffer);
		}
		if (buffer[0]==0x00)
		{
			HanldeOkPacket(buffer,packetSequenceNumber);
		}

	}
	
	private void HandleErrorPacket(ubyte[]packet)
	{
		//first byte is 0xff, error indicator. Since the call was passed here we assume its value and skip it
		packet = packet[1..$];

		ushort errorCode = read!(ushort,endian.littleEndian)(packet);

		//skip state marker and state until we need them
		packet = packet[6..$];
		string errorMessage = ReadString(packet);
		throw new MySqlDException(errorMessage,errorCode);
		
	}
	
	private void HanldeOkPacket(ubyte[]packet,uint packetSequenceNumber)
	{
		//first byte is 0x00, ok indicator. Since the call was passed here we assume its value and skip it
		packet = packet[1..$];
		//TODO: Create a struct to hold the OK packet results
		ulong rowsAffected = ReadLengthEncodedInteger(packet);
		ulong lastInserId = ReadLengthEncodedInteger(packet);
		ushort status = read!(ushort,endian.littleEndian)(packet);
		ushort numberOfWarnings = read!(ushort,endian.littleEndian)(packet);

	}
	private PreparedStatement PrepareStatement(string statement)
	{
		//create an alias for _TempBuffer to slice easily without _TempBuffer gets affected
		ubyte[] preparedStatementPacket = _TempBuffer;

		//first 4 bytes are for packet header that we will write at the end of this method
		uint currentIndex =4;

		preparedStatementPacket[currentIndex]= PreparedStatementCommands.COM_STMT_PREPARE;
		currentIndex++;
		WriteString(preparedStatementPacket,statement,currentIndex);

		//packet length exclues the 4 bytes packet header
		uint packetLength = currentIndex -4;
		/*write the packet length in the first 4 bytes (packet header). The protocol specifies that only 3 bytes are for the packet length and the forth is for the packet sequence. 
		We will overwrite the forth byte later*/
		write!(uint,Endian.littleEndian)(preparedStatementPacket,packetLength,0);
		//write the packet sequeence in the forth byte
		preparedStatementPacket[3]=0;

		_Socket.send(preparedStatementPacket[0..currentIndex]);
		return GetPrepareStatementCommandResult();

	}
	private PreparedStatement GetPrepareStatementCommandResult()
	{
		//create an alias for _TempBuffer to slice easily without _TempBuffer gets affected
		ubyte[] responseBuffer = _TempBuffer;
		_Socket.receive(responseBuffer);

		//packet size is found in the first three bytes
		ubyte[4] packetSizeBytes;
		packetSizeBytes[0..3]= responseBuffer[0..3];
		//add a forth byte to be able to convert to a uint
		packetSizeBytes[3]=0;

		uint packetSize = peek! (uint,Endian.littleEndian)(cast (ubyte[]) packetSizeBytes);
		//remove bytes we have already consumed
		responseBuffer = responseBuffer[3..$];

		uint packetSequenceNumber = cast (uint) responseBuffer[0];
		//once again remove bytes we have consumed
		responseBuffer = responseBuffer[1..$];

		//remove useless bytes
		responseBuffer = responseBuffer[0..packetSize-1];

		if (responseBuffer[0]==0x00)
		{
			//remove status byte
			responseBuffer = responseBuffer[1..$];
			uint statementId = read!(uint,Endian.littleEndian)(responseBuffer);
			ushort columnsCount = read!(ushort,Endian.littleEndian)(responseBuffer);
			ushort parametersCount = read! (ushort,Endian.littleEndian)(responseBuffer);
			ushort warningsCount = read !(ushort,Endian.littleEndian)(responseBuffer);
			PreparedStatement statement =  PreparedStatement(statementId,this,parametersCount);
			statement.ColumnsCount = columnsCount;
			statement.WarningsCount = warningsCount;
			return statement;
		}
		else if (responseBuffer[0]== 0xff)
		{
			HandleErrorPacket (responseBuffer);
		}

		MySqlDException ex = new MySqlDException("Undefined Response");
		ex.ServerResponse = responseBuffer;
		throw ex;

	}
	private void ClosePreparedStatement(uint statementId)
	{
		/*write the packet length in the first 4 bytes (packet header). The protocol specifies that only 3 bytes are for the packet length and the forth is for the packet sequence. 
		We will overwrite the forth byte later*/
		write!(uint,Endian.littleEndian)(_TempBuffer,5,0);
		//write the packet sequeence in the forth byte
		_TempBuffer[3]=0;

		//first 4 bytes are for packet header
		uint currentIndex =4;

		_TempBuffer[currentIndex]= PreparedStatementCommands.COM_STMT_CLOSE;
		currentIndex++;
		write!(uint,Endian.littleEndian)(_TempBuffer,statementId,currentIndex);
		currentIndex += 4;
		_Socket.send(_TempBuffer[0..currentIndex]);
	}
	
	void Disconnect()
	{
		_Socket.shutdown(SocketShutdown.BOTH);
		_Socket.close();
	}
	

}
extern class ConnectionParameters
{
	private string _DatabaseName;
	private ushort _Port;
	private string _ServerAddress;
	private string _Username;
	private string _Password;

	@property
	{
		public string DatabaseName()
		{
			return _DatabaseName;
		}
		public string DatabaseName(string newDatabaseName)
		{
			return _DatabaseName = newDatabaseName;
		}
		public ushort Port()
		{
			return _Port;
		}
		public ushort Port(ushort newPort)
		{
			return _Port = newPort;
		}
		public string ServerAddress()
		{
			return _ServerAddress;
		}
		public string ServerAddress (string newServerAddress)
		{
			return _ServerAddress = newServerAddress;
		}
		public string Username()
		{
			return _Username;
		}
		public string Username(string newUsername)
		{
			return _Username = newUsername;
		}
		public string Password()
		{
			return _Password;
		}
		public string Password(string newPassword)
		{
			return _Password = newPassword;
		}
	}

}

extern class MySqlDException:Exception
{
	private uint _ErrorCode;
	private ubyte[] _ServerResponse;

	this(string message)
	{
		super(message);
	}
	this (string message,ushort errorCode)
	{
		super(message);
		ErrorCode = errorCode;
	}
	@property
	{
		public uint ErrorCode()
		{
			return _ErrorCode;
		}
		public uint ErrorCode(uint errorCode)
		{
			return _ErrorCode = errorCode;
		}
		public ubyte[] ServerResponse(ubyte[] response)
		{
			return _ServerResponse = response;
		}
		public ubyte[] ServerResponse()
		{
			return _ServerResponse;
		}
	}

}

extern class DatabaseHandler
{
	private Connection _Connection;
	private ConnectionParameters _Parameters; 
	public this(ConnectionParameters parameters)
	{
		_Parameters = parameters;
	}
	public void Connect()
	{
		if (_Connection is null)
			_Connection = new Connection();
		ConnectionParameters parameters = new ConnectionParameters();
		_Connection.Connect(_Parameters);
	}
	public PreparedStatement PrepareStatement(string statement)
	{
		return _Connection.PrepareStatement(statement);
	}

}
/**********************************************************
A struct holding data about a prepared statement and allows for its execution and closing. 
This struct is an appliction for the RAII principle, where the statement is closed already when the object is destroyed if it was not already closed. Uncolsed statements is a commor reason for resources and memory leak
on the server.
*/
extern struct PreparedStatement
{
	private bool _IsClosed = false;
	private uint _Id;
	private ushort _NumberOfParameters;
	private ushort _ColumnsCount;
	private ushort _WarningsCount;
	@property
	{
		private ushort WarningsCount(ushort warningsCount)
		{
			return _WarningsCount = warningsCount;
		}
		public ushort WarningsCount()
		{
			return _WarningsCount;
		}


		private ushort ColumnsCount(ushort columnsCount)
		{
			return _ColumnsCount = columnsCount;
		}
		public ushort ColumnsCount()
		{
			return _ColumnsCount;
		}
	}

	private Connection _Connection; 
	//@disable this();
	this(uint statementId,Connection connection,ushort numberOfParameters)
	{
		_Id = statementId;
		_Connection = connection;
		_NumberOfParameters = numberOfParameters;
	}
	~this()
	{
		if (!_IsClosed)
			Close();
	}
	public void Close()
	{
		if (_IsClosed)
			throw new MySqlDException("Statement is already closed");
		_Connection.ClosePreparedStatement(_Id);
	}
}

unittest{
	
	import TestingHelper;
	import std.conv;


	YamlFile configurationFile = new YamlFile();
	configurationFile.Open("TestConfig.yaml");


	ConnectionParameters parameters = new ConnectionParameters();
	parameters.ServerAddress = configurationFile.GetValue("ServerAddress");
	parameters.Port = to!ushort (configurationFile.GetValue("Port") );
	parameters.Username = configurationFile.GetValue("Username");
	parameters.Password = configurationFile.GetValue("Password");
	DatabaseHandler dbh = new DatabaseHandler(parameters);
	dbh.Connect();
	dbh.PrepareStatement("CREATE DATABASE test");
	
	
}

