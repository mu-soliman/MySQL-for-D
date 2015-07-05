module MySqlForD.PreparedStatement;

import std.variant;
import std.exception;

public import MySqlForD.CommandResult;
public import MySqlForD.Connection;
import MySqlForD.Exceptions;


/**********************************************************
A struct holding data about a prepared statement and allows for its execution and closing. 
This struct is an appliction for the RAII principle, where the statement is closed already when the object is destroyed if it was not already closed. Uncolsed statements is a commor reason for resources and memory leak
on the server.
It was put in this file because its constructor is private and can be accessed only by InternalConnection class
*/
extern struct PreparedStatement
{
	private bool _IsClosed = false;
	private uint _Id;
	private ushort _NumberOfParameters;
	private ushort _ColumnsCount;
	private ushort _WarningsCount;
	private Connection _Connection; 

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

		/*************************
		The Connection object that will be used to execute and close the prepared statement
		*/
		public Connection AssociatedConnection (Connection connection)
		{
			return _Connection = connection;
		}
		public  Connection AssociatedConnection ()
		{
			return _Connection;
		}
	}

	

	this(uint statementId,ushort parametersCount,ushort columsCount,ushort warningCount)
	{
		_Id = statementId;
		_WarningsCount = warningCount;
		_NumberOfParameters = parametersCount;
		_ColumnsCount = columsCount;
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
		_IsClosed = true;
	}
	public CommandResult ExecuteCommand(Variant[] parametersValues = null)
	{
		if (_NumberOfParameters !=0)
		{
			enforce(parametersValues!=null && parametersValues.length ==_NumberOfParameters,new MySqlDException("Number of parameters passed doesn't match statement's parameters count"));
		}
		return _Connection.ExecuteCommandPreparedStatement(_Id,parametersValues);
	}

}

