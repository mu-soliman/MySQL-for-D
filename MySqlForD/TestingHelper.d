module TestingHelper;
import std.stdio;
import std.range;
import std.string;

/**********************************
A class created meanly for reading test configuration files. The files are written in simple YAML format, however, this class doesn't handle the YAML format precisely. It is built mainly to fulfill the task
of loading test configuration files
*/

class YamlFile
{
	
	public void Open(string fileName)
	{
		_File = File(fileName);
		auto range = _File.byLine();

		foreach (s;range)
		{
			string line = cast(string) s;
			string strippedLine = strip (line);
			string [] words = line.split(":");

			_Entries[ strip(words[0]).idup ]= strip( words[1] ).idup;

		}
	}
	public string GetValue(string key)
	{
		return _Entries[key];
	}


	~this()
	{
		if (_File.isOpen())
			_File.close();
	}

	private File _File;
	private string[string]_Entries;
	
}
