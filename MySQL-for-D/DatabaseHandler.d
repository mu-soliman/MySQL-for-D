module DatabaseHandler;
import std.socket;
import std.bitmanip;
import std.system;
import std.stdio;
import Common.Functions;
import Common.Exceptions;

/***********************************
This is a connection that should be pooled later (didn't write pooling code yet). This is why I had to create a connection object separate from the database handler object.
*/
private class Connection {
	static const uint AUTH_PLUGIN_DATA_PART1_LENGTH = 8;
	static const uint RESERVED_STRING_LENGTH = 10;


	private uint		_ProtocolVersion;
	private Socket		_Socket;
	private string		_ServerVersion;
	private uint		_ConnectionId;
	private string		_AuthPluginName;
	private ubyte		_ServerCharacterSet;
	private ubyte[4]	_ServerCapabilities;
	private ubyte[2]	_ServerStatus;

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

	public void Connect(string domain, ushort port)
	{

		_Socket.connect(new InternetAddress(domain,port));
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
			ProcessInitialHandshakeMessage(buffer);

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

		_ServerCapabilities[0..2]= initialHandshakeMessage[0..2];
		initialHandshakeMessage = initialHandshakeMessage[2..$];

		_ServerCharacterSet = initialHandshakeMessage[0];
		initialHandshakeMessage = initialHandshakeMessage[1..$];

		_ServerStatus = initialHandshakeMessage[0..2];
		initialHandshakeMessage = initialHandshakeMessage[2..$];

		_ServerCapabilities[2..4] = initialHandshakeMessage[0..2];
		initialHandshakeMessage = initialHandshakeMessage[2..$];

		uint totalLentgthOfAuthPluginData = initialHandshakeMessage[0];
		//remove bytes we have consumed
		initialHandshakeMessage = initialHandshakeMessage[1..$];

		//skip empty reserved string
		initialHandshakeMessage = initialHandshakeMessage[RESERVED_STRING_LENGTH .. $];
		
		uint lengthOfAuthPluginDataPart2 = totalLentgthOfAuthPluginData -8;
		ubyte[]authPluginDataPart2=  initialHandshakeMessage[0 .. lengthOfAuthPluginDataPart2];
		//remove bytes we consumed
		initialHandshakeMessage = initialHandshakeMessage[lengthOfAuthPluginDataPart2..$];

		_AuthPluginName = ReadString(initialHandshakeMessage);
		writefln(_AuthPluginName);
		write("Bye");

	}
	void Disconnect()
	{
	}

}


extern class DatabaseHandler
{
	private Connection _Connection;
	public void Connect(string domain, ushort port)
	{
		if (_Connection is null)
			_Connection = new Connection();
		_Connection.Connect(domain,port);
	}

}
unittest{
	
	DatabaseHandler dbh = new DatabaseHandler();
	dbh.Connect("127.0.0.1",3306);
	
}
