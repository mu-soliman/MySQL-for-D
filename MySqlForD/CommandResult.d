module MySqlForD.CommandResult;

/****************************************************************
A class holding the results of a successfully executed commmand. A command is either an insert, update or a delete, it cannot be a select statement
*/
class CommandResult
{
	@property
	{
		public long LastInsertedId(long id)
		{
			return _LastInsertedId = id;
		}
		public long InsertedId()
		{
			return _LastInsertedId;
		}

		public ulong NumberOfRowsAffected(ulong numberOfRowsAffected)
		{
			return _NumberOfRowsAffected = numberOfRowsAffected;
		}
		public ulong NumberOfRowsAffected ()
		{
			return _NumberOfRowsAffected;
		}
		public ushort NumberOfWarnings(ushort numberOfWarnings)
		{
			return _NumberOfWarnings = numberOfWarnings;
		}
		public ushort NumberOfWarnings()
		{
			return _NumberOfWarnings;
		}
	}
	private long _LastInsertedId;
	private ulong _NumberOfRowsAffected;
	private ushort _NumberOfWarnings;
}