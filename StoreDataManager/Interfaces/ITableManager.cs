using Entities;

namespace StoreDataManager.Interfaces
{
    public interface ITableManager
    {
        OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions);
        (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName);
    }
}
