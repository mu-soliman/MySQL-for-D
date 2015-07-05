module MySqlForD.Connection;
import std.variant;
import MySqlForD.ConnectionParameters;
private import MySqlForD.InternalConnection;
import MySqlForD.Exceptions;
public import  MySqlForD.PreparedStatement;

extern class Connection
{
	private InternalConnection _InternalConnection;
	private ConnectionParameters _Parameters; 
	public this(ConnectionParameters parameters)
	{
		_Parameters = parameters;
	}

	public void Connect()
	{
		if (_InternalConnection is null)
			_InternalConnection = new InternalConnection();
		ConnectionParameters parameters = new ConnectionParameters();
		_InternalConnection.Connect(_Parameters);
	}

	public PreparedStatement PrepareStatement(string statement)
	{
		_CheckInternalConnection();
		PreparedStatement preparedStatement =  _InternalConnection.PrepareStatement(statement);
		preparedStatement.AssociatedConnection = this;
		return preparedStatement;
		
	}

	public void ClosePreparedStatement(uint statementId)
	{
		_CheckInternalConnection();
		_InternalConnection.ClosePreparedStatement(statementId);
	}
	public CommandResult ExecuteCommandPreparedStatement(uint statementId,Variant[] parameters = null)
	{
		_CheckInternalConnection();
		return _InternalConnection.ExecuteCommandPreparedStatement(statementId,parameters);
	}

	public void Disconnect()
	{

	}
	/*****************************************************************
	Make sure that the internal connection object is created and open
	*/
	private void _CheckInternalConnection()
	{
		if (!_InternalConnection)
			throw new MySqlDException("No internal connection associated with this connection");

		if (!_InternalConnection.IsConnected())
			throw new MySqlDException ("Connection is disconnected");
	}

}
