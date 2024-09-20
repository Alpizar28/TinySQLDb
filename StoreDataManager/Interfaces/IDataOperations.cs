using System.Collections.Generic;
using Entities;

namespace StoreDataManager.Interfaces
{
    public interface IDataOperations
    {
        OperationStatus InsertRow(string databaseName, string tableName, string[] columns, string[] rowValues);
        List<Dictionary<string, string>> SelectAll(string databaseName, string tableName);
    }
}
