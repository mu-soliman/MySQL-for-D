module DatabaseHandler;
import std.socket;
import std.bitmanip;
import std.system;
import std.stdio;
import Common.Functions;
import Common.Exceptions;


extern class DatabaseHandler
{
	private uint _ProtocolVersion;
	private Socket _Socket;
	private string _ServerVersion;

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
		 packetSizeBytes[0..2]= buffer[0..2];
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
			 ProcessHandshakeMessage(buffer);
		 
	}
	private void ProcessErrorMessage (ubyte[] error)
	{
		
	}
	private void ProcessHandshakeMessage(ubyte[] handshakeMessage)
	{
		_ProtocolVersion = handshakeMessage[0];
		//remove bytes we have consumed
		handshakeMessage = handshakeMessage[1..$];
		_ServerVersion = ReadString(handshakeMessage);
		
	}
	void Disconnect()
	{
	}
	

}
unittest{
	
	DatabaseHandler dbh = new DatabaseHandler();
	dbh.Connect("127.0.0.1",3306);
	
}
