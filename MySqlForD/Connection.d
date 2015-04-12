module MySqlForD.Connection;

import MySqlForD.ConnectionParameters;
import MySqlForD.RAII;
import MySqlForD.Exceptions;

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
		if (!_InternalConnection || !_InternalConnection.IsConnected())
		{
			throw new MySqlDException ("Connection is disconnected");
		}
		return _InternalConnection.PrepareStatement(statement);
	}
	public void Disconnect()
	{

	}

}
