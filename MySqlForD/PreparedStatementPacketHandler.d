module MySqlForD.PreparedStatementPacketHandler;

import std.bitmanip;
import std.system;
import std.variant;
import std.datetime;
import std.stdio;

import MySqlForD.Exceptions;
import MySqlForD.Functions;
import MySqlForD.PreparedStatement;
import MySqlForD.PacketHandler;

class PreparedStatementPacketHandler:PacketHandler
{
	private enum PreparedStatementCommands
	{
		COM_STMT_PREPARE = 22,
		COM_STMT_EXECUTE = 23,
		COM_STMT_SEND_LONG_DATA = 24,
		COM_STMT_CLOSE = 25

	}

	/***********************************************************************
	Generate packet for COM_STMT_PREPARE command. The packet is written into buffer 

	Returns: The length of the packet
	*/
	public uint GeneratePrepareStatementPacket(ubyte[] buffer,string statement)
	{
		uint currentIndex = 0;
		buffer[currentIndex]= PreparedStatementCommands.COM_STMT_PREPARE;
		currentIndex++;
		WriteString(buffer,statement,currentIndex);
		return currentIndex;
	}


	/************************************************

	Generate packet for COM_STMT_EXECUTE command. The packet is wrtten into buffer

	Returns: The length of the packet

	*/
	public uint GeneratePreparedStatementExecutePacket(ubyte[] buffer,uint statementId,Variant[] parameters = null)
	{
		
		uint currentIndex = 0;
		buffer[currentIndex] = PreparedStatementCommands.COM_STMT_EXECUTE;
		currentIndex++;

		write!(uint,Endian.littleEndian)(buffer,statementId,currentIndex);
		currentIndex += 4;

		//no flags to set for now
		buffer[currentIndex]=0;
		currentIndex++;

		//iteration count is always 1. it is stored in 4 bytes
		buffer[currentIndex]=1;
		currentIndex += 4;

		if (parameters != null && parameters.length > 0)
		{
			//generate null bitmap
			ubyte[] nullBitmap;
			nullBitmap.length = (parameters.length+7)/8;
			int numberOfNullParameters = 0;
			int totalSizeOfParameters = 0;
			foreach(index,parameter;parameters)
			{
				if (parameter.hasValue() == true)
				{
					totalSizeOfParameters += GetVariantSize(parameter);
					continue;
				}
				numberOfNullParameters++;
				uint byteIndex = cast (uint) index / 8;
				uint bitIndex = index % 8;
				nullBitmap[byteIndex] |=  (1 << bitIndex);
			}
			buffer[currentIndex..currentIndex+nullBitmap.length]=nullBitmap;
			currentIndex += nullBitmap.length;

			//I searched online and couldn't find what this parameters does exactly, just following the documentation blindly
			ubyte newParamsBoundFlag = 1;
			buffer[currentIndex] = newParamsBoundFlag;
			currentIndex++;

			//insert parameters values based on types
			ubyte[]parametersTypes;
			parametersTypes.length = (parameters.length - numberOfNullParameters) *2;
			ubyte[]parametersValues;
			parametersValues.length = totalSizeOfParameters;
			uint parameterIndexInValuesByteArray = 0;
			foreach(index,parameter;parameters)
			{
				if (!parameter.hasValue())
					continue;
				ushort typeHexadecimalValue = GetTypeHexadecimalValue(parameter.type);
				write!(ushort,Endian.littleEndian)(parametersTypes,typeHexadecimalValue,index*2);
				WriteVariant(parametersValues, parameterIndexInValuesByteArray,parameter);

			}
			buffer[currentIndex..currentIndex+parametersTypes.length]=parametersTypes;
			currentIndex+= parametersTypes.length;

			buffer[currentIndex..currentIndex+parametersValues.length]=parametersValues;
			currentIndex+=parametersValues.length;

		}		
		return currentIndex;
	}
	public PreparedStatement ParsePrepareStatementResponseFirstPacket(ubyte[] buffer)
	{		
		if (buffer[0]==0x00)
		{
			//remove status byte
			buffer = buffer[1..$];
			uint statementId = read!(uint,Endian.littleEndian)(buffer);
			ushort columnsCount = read!(ushort,Endian.littleEndian)(buffer);
			ushort parametersCount = read! (ushort,Endian.littleEndian)(buffer);
			ushort warningsCount = read !(ushort,Endian.littleEndian)(buffer);
			PreparedStatement statement =  PreparedStatement(statementId,parametersCount,columnsCount,warningsCount);
			return statement;
		}
		else if (buffer[0]== 0xff)
		{
			ParseErrorPacket (buffer);
		}

		MySqlDException ex = new MySqlDException("Unsupported Response");
		ex.ServerResponse = buffer;
		throw ex;

	}

	
	private  ushort  GetTypeHexadecimalValue(TypeInfo myType) 
	{
		if (myType == typeid(byte))
			return 0x01;
		if (myType == typeid(short))
			return 0x02;
		if (myType == typeid(int))
			return 0x03;
		if (myType == typeid(float))
			return 0x04;
		if (myType == typeid(double))
			return 0x05;
		if (myType == typeid(DateTime))
			return 0x07;
		if (myType == typeid(long))
			return 0x08;
		if (myType == typeid(string))
			return 0xfe;

		throw new  InvalidArgumentException("Unknown type passed");
	}
	/***********************************************************************
	Write a variant variable into buffer variable at a given index. The supported variable types are 
	*/
	private void WriteVariant(ubyte[]buffer,ref uint index,Variant value)
	{
		if (value.type == typeid(byte))
		{
			byte byteValue = value.get!(byte);
			write!(byte,Endian.littleEndian)(buffer,byteValue,index);
			index += byte.sizeof;
			return;
		}

		if (value.type == typeid(short))
		{
			short shortValue = value.get!(short);
			write!(short,Endian.littleEndian)(buffer,shortValue,index);
			index += short.sizeof;
			return;
		}

		if (value.type == typeid(int))
		{
			int intValue = value.get!(int);
			write!(int,Endian.littleEndian)(buffer,intValue,index);
			index += int.sizeof;
			return;
		}

		if (value.type == typeid(float))
		{
			float floatValue = value.get!(float);
			write!(float,Endian.littleEndian)(buffer,floatValue,index);
			index += float.sizeof;
			return;
		}

		if (value.type == typeid(double))
		{
			double doubleValue = value.get!(double);
			write!(double,Endian.littleEndian)(buffer,doubleValue,index);
			index += double.sizeof;
			return;
		}

		if (value.type == typeid(DateTime))
		{
			DateTime dateTimeValue = value.get!(DateTime);
			//write the length of the date time. Datetime doesn't has milliseconds property, so we can only send the year,month,day, hours,minutes,seconds
			write!(ubyte,Endian.littleEndian)(buffer,7,index);
			index += ubyte.sizeof;

			write!(ushort,Endian.littleEndian)(buffer,dateTimeValue.year,index);
			index += ushort.sizeof;

			write!(ubyte,Endian.littleEndian)(buffer,dateTimeValue.month,index);
			index += ubyte.sizeof;

			write!(ubyte,Endian.littleEndian)(buffer,dateTimeValue.day,index);
			index += ubyte.sizeof;

			write!(ubyte,Endian.littleEndian)(buffer,dateTimeValue.hour,index);
			index += ubyte.sizeof;

			write!(ubyte,Endian.littleEndian)(buffer,dateTimeValue.minute,index);
			index += ubyte.sizeof;

			write!(ubyte,Endian.littleEndian)(buffer,dateTimeValue.second,index);
			index += ubyte.sizeof;

			return;

		}

		if (value.type == typeid(long))
		{
			long longValue = value.get!(long);
			write!(long,Endian.littleEndian)(buffer,longValue,index);
			index += long.sizeof;
			return;
		}

		if (value.type == typeid(string))
		{
			string stringValue = value.get!(string);
			//write string length as a length encode integer
			ubyte[] stringLength = ConvertToLengthEncodedInteger(stringValue.length);
			buffer[index..index+stringLength.length]=stringLength;
			index +=stringLength.length;
			WriteString(buffer,stringValue,index);
			return;
		}
	}

}
