using Entities;
using System.Collections.Generic;

namespace StoreDataManager.Interfaces
{
    public interface ITableManager
    {
        OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions);
        (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName);
        bool IsTableEmpty(string databaseName, string tableName);
        OperationStatus DropTable(string databaseName, string tableName);
        OperationStatus CreateIndex(string databaseName, string tableName, string columnName, string indexName, string indexType);
        List<Dictionary<string, string>> SearchUsingIndex(string databaseName, string tableName, string columnName, string searchValue);
        List<Dictionary<string, string>> SearchSequentially(string databaseName, string tableName, string columnName, string searchValue);
        string GetIndexInfo(string databaseName, string tableName, string columnName);
        OperationStatus DeleteRows(string databaseName, string tableName, string columnName, string value);
        OperationStatus DeleteAllRows(string databaseName, string tableName);
        OperationStatus UpdateAllRows(string databaseName, string tableName, string columnName, string newValue);

        OperationStatus UpdateRows(string databaseName, string tableName, string columnToUpdate, string newValue, string conditionColumn, string conditionValue);

    }
}
