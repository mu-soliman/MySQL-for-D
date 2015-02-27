/***********************
Module to contain all helper general purpose functions
*/
module Common.Functions;
import Common.Exceptions;


/************************************
*Read a null terminated string from an unsigned byte array. If it is not null terminated it reads till the end of the array

*Reading starts fron index 0 till the first null terminated string. The bytes that are consumed are removed from the input byte array

* Throws InvalidArgumentException if input contained no null string
*/
string ReadString(ref ubyte[] input)
{
	ulong indexOfLastCharacter = input.length -1;
	foreach(i, b; input)
	{
		if (b =='\0')
		{
			indexOfLastCharacter = i;
			break;
		}
	}
	
	char[] characters = cast (char[]) input[0..indexOfLastCharacter+1];
	
	//remove consumed characters from input array 
	input = input[indexOfLastCharacter+1 .. $];

	return cast (string) characters;
}

