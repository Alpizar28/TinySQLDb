using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string databaseName, string tableName, string[] columnDefinitions)
        {
            return Store.GetInstance().CreateTable(databaseName, tableName, columnDefinitions);
        }
    }
}
