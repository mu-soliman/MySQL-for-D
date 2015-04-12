/************************************
*A module to include general purpose exceptions. 

*Here I am affected by the .NET standard set of exceptions build in the framework which I thought I would find a similar set of exception in D's standard library.
*May be I didn't search enough
*/
module MySqlForD.Exceptions;

export class InvalidArgumentException:Exception
{
	this(string message)
	{
		super(message);
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
