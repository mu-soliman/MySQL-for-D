/***********************
Module to contain all helper general purpose functions
*/
module Common.Functions;
import Common.Exceptions;


/************************************
*Read a null terminated string from an unsigned byte array. 

*Reading starts fron index 0 till the first null terminated string. The bytes that are consumed are removed from the input byte array

* Throws InvalidArgumentException if input contained no null string
*/
string ReadString(ref ubyte[] input)
{
	ulong indexOfFirstNullCharacter = 0;
	bool nullStringFound = false;

	foreach(i, b; input)
	{
		if (b =='\0')
		{
			indexOfFirstNullCharacter = i;
			nullStringFound = true;
			break;
		}
	}
	if (!nullStringFound)
	{
		throw new InvalidArgumentException("No null character in input");
	}
	
	char[] characters = cast (char[]) input[0..indexOfFirstNullCharacter];
	
	//remove consumed characters from input array 
	input = input[indexOfFirstNullCharacter+1 .. $];

	return cast (string) characters;
}

