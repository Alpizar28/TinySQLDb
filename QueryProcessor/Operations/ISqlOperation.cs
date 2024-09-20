using Entities;

namespace QueryProcessor.Operations
{
    public interface ISqlOperation
    {
        OperationStatus Execute(string query, ref string currentDatabaseName);
    }
}
