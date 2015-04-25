module MySqlForD.ConnectionParameters;


extern class ConnectionParameters
{
	private string _DatabaseName;
	private ushort _Port;
	private string _ServerAddress;
	private string _Username;
	private string _Password;

	@property
	{
		pure public string DatabaseName()
		{
			return _DatabaseName;
		}
		pure public string DatabaseName(string newDatabaseName)
		{
			return _DatabaseName = newDatabaseName;
		}
		pure public ushort Port()
		{
			return _Port;
		}
		pure public ushort Port(ushort newPort)
		{
			return _Port = newPort;
		}
		pure public string ServerAddress()
		{
			return _ServerAddress;
		}
		pure public string ServerAddress (string newServerAddress)
		{
			return _ServerAddress = newServerAddress;
		}
		pure public string Username()
		{
			return _Username;
		}
		pure public string Username(string newUsername)
		{
			return _Username = newUsername;
		}
		pure public string Password()
		{
			return _Password;
		}
		pure public string Password(string newPassword)
		{
			return _Password = newPassword;
		}
	}

}