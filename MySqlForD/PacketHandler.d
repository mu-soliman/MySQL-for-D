module MySqlForD.PacketHandler;

import std.bitmanip;
import std.system;

import MySqlForD.Exceptions;
import MySqlForD.Functions;

abstract class PacketHandler
{
	protected static const uint PACKT_HEADER_LENGTH	=	4;




	/********************************************************************
	Extract packet info from the packet and remove the packet header bytes from the input byte array
	*/
	protected PacketHeader ExtractPacketHeader(ref ubyte[] buffer)
	{
		//packet size is found in the first three bytes
		ubyte[4] packetSizeBytes;
		packetSizeBytes[0..3]= buffer[0..3];
		//add a forth byte to be able to convert to a uint
		packetSizeBytes[3]=0;

		uint packetSize = peek! (uint,Endian.littleEndian)(cast (ubyte[]) packetSizeBytes);
		//remove bytes we have already consumed
		buffer = buffer[3..$];

		ubyte packetSequenceNumber =  buffer[0];
		//once again remove bytes we have consumed
		buffer = buffer[1..$];

		PacketHeader header = new PacketHeader(packetSize,packetSequenceNumber);
		return header;
	}

	protected void ParseErrorPacket(ubyte[]packet)
	{
		//first byte is 0xff, error indicator. Since the call was passed here we assume its value and skip it
		packet = packet[1..$];

		ushort errorCode = read!(ushort,endian.littleEndian)(packet);

		//skip state marker and state until we need them
		packet = packet[6..$];
		string errorMessage = ReadString(packet);
		throw new MySqlDException(errorMessage,errorCode);

	}

	/***************************************************************
	Add packet header to a buffer byte array
	*/
	protected void AddPacketHeader(ref ubyte[] buffer, uint packetSize,ubyte packetSequence)
	{
		write!(uint,Endian.littleEndian)(buffer,packetSize,0);
		//write the packet sequence in the forth byte
		buffer[3]=packetSequence;
	}

}

class PacketHeader
{
	public uint PacketLength;
	public ubyte PacketSequence;
	this()
	{

	}
	this(uint packetLength,ubyte packetSequence)
	{
		PacketLength = packetLength;
		PacketSequence = packetSequence;
	}
}

