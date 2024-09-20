using System;

namespace QueryProcessor.Exceptions
{
    public class UnknownSQLSentenceException : Exception
    {
        public UnknownSQLSentenceException()
        {
        }

        public UnknownSQLSentenceException(string message)
            : base(message)
        {
        }

        public UnknownSQLSentenceException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
