module DatabaseHandler;
import std.socket;
import std.bitmanip;
import std.system;
import std.stdio;
import Common.Functions;
import Common.Exceptions;

/***********************************
This is a connection that should be pooled later (didn't write pooling code yet). This is why I had to create a connection object separate from the database handler object. This pooling should be transparent to the database
handler user, this is why it is private class
*/
private class Connection {

	static const uint AUTH_PLUGIN_DATA_PART1_LENGTH			=	8;
	static const uint RESERVED_SERVER_STRING_LENGTH			=	10;
	static const uint SERVER_STATUS_LENGTH					=	2;

	static const uint RESERVED_CLIENT_STRING_LENGTH			=	23;

	private ConnectionParameters	_ConnectionParameters;
	private uint					_ProtocolVersion;
	private Socket					_Socket;
	private string					_ServerVersion;
	private uint					_ConnectionId;
	private string					_AuthPluginName;
	private ubyte					_ServerCharacterSet;
	private uint					_ServerCapabilities;
	private ubyte[2]				_ServerStatus;

	private enum CapabilityFlags
	{
		CLIENT_PROTOCOL_41		=	0x00000200,
		CLIENT_LONG_PASSWORD	=	0x00000001,
		CLIENT_CONNECT_WITH_DB  =   0x00000008 
	}

	public @property uint ProtocolVersion()
	{
		return _ProtocolVersion;
	}
	public @property ServerVersion ()
	{
		return _ServerVersion;
	}
	public this()
	{
		_Socket = new TcpSocket();
	}

	public void Connect(ConnectionParameters parameters)
	{
		_ConnectionParameters = parameters;
		_Socket.connect(new InternetAddress(parameters.ServerAddress,parameters.Port));
		ubyte[] buffer;
		buffer.length = 256;
		_Socket.receive(buffer);

		//packet size is found in the first three bytes
		ubyte[4] packetSizeBytes;
		packetSizeBytes[0..3]= buffer[0..3];
		//add a forth byte to be able to convert to a uint
		packetSizeBytes[3]=0;
		uint packetSize = peek! (uint,Endian.littleEndian)(cast (ubyte[]) packetSizeBytes);

		//remove useless bytes
		buffer = buffer[0..packetSize-1];
		//remove bytes we have already consumed
		buffer = buffer[3..$];

		uint packetSequenceNumber = cast (uint) buffer[0];
		//once again remove bytes we have consumed
		buffer = buffer[1..$];

		ushort initial_byte = buffer[0];

		if (initial_byte == 0xFF)
			ProcessErrorMessage(buffer);
		else
		{
			ProcessInitialHandshakeMessage(buffer);
			SendHandshakeResponseMessage();
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

		ubyte[] authPluginDataPart1 = initialHandshakeMessage[0..AUTH_PLUGIN_DATA_PART1_LENGTH];
		//remove bytes we have consumed
		initialHandshakeMessage = initialHandshakeMessage[AUTH_PLUGIN_DATA_PART1_LENGTH..$];
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

		_AuthPluginName = ReadString(initialHandshakeMessage);

	}

	private void SendHandshakeResponseMessage()
	{
		ubyte[] handshakeResponseMessage;
		//calculate the lenth of the message here

		handshakeResponseMessage.length = 100;
		uint currentIndex =0;
		//the first 3 bytes if for the packet size. The forth is for the packet sequence
		currentIndex += 4;

		uint capabilities =GenerateCapabilityFlags();
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,capabilities,currentIndex);
		currentIndex += 4;

		//maximum size for a command that we may send to the database. I don't know the criteria based on which I can specify this number, so I will set it not for 4096 until I know better way to specify this number
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,4096,currentIndex);
		currentIndex +=4;

		//use the default character set for mysql which is latin1_swedish_ci 
		write!(ubyte,Endian.littleEndian)(handshakeResponseMessage, 0x08 ,currentIndex);
		currentIndex ++;

		//skip reserved client string
		currentIndex += RESERVED_CLIENT_STRING_LENGTH;

		//strings in D are not null terminated and the protocol expected a null terminated string for the username
		string userName = _ConnectionParameters.Username ~ "\0";
		WriteString(handshakeResponseMessage,userName,currentIndex);

		string password = _ConnectionParameters.Password ~"\0";
		WriteString(handshakeResponseMessage,password,currentIndex);
		
		uint x = _ServerCapabilities & CapabilityFlags.CLIENT_CONNECT_WITH_DB;
		
		if (_ConnectionParameters.DatabaseName.length >0 && (_ServerCapabilities & CapabilityFlags.CLIENT_CONNECT_WITH_DB))
		{
			string databaseName = _ConnectionParameters.DatabaseName ~ "\0";
			WriteString(handshakeResponseMessage,databaseName,currentIndex);
		}
		//write the packet length in the first 4 bytes. The protocol specifies that only 3 bytes are for the packet length and the forth is for the packet sequence. We will overwrite the forth byte later
		write!(uint,Endian.littleEndian)(handshakeResponseMessage,currentIndex,0);
		//write the packet sequeence in the forth byte
		handshakeResponseMessage[3]=0;


	}
	private uint GenerateCapabilityFlags()
	{
		uint capabilities =0;
		capabilities = capabilities | CapabilityFlags.CLIENT_PROTOCOL_41;
		capabilities = capabilities | CapabilityFlags.CLIENT_LONG_PASSWORD;
		if ( _ConnectionParameters.DatabaseName.length > 0)
			capabilities = capabilities | CapabilityFlags.CLIENT_CONNECT_WITH_DB;

		return capabilities;
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
	
}
