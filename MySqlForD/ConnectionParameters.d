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