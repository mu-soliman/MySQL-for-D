/************************************
*A module to include general purpose exceptions. 

*Here I am affected by the .NET standard set of exceptions build in the framework which I thought I would find a similar set of exception in D's standard library.
*May be I didn't search enough
*/
module Common.Exceptions;

export class InvalidArgumentException:Exception
{
	this(string message)
	{
		super(message);
	}
}