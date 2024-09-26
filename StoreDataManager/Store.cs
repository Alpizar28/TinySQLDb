using StoreDataManager.Interfaces;
using StoreDataManager.Implementations;
using Entities;
using System.Collections.Generic;
using System.IO;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        private readonly IDatabaseManager _databaseManager;
        private readonly ITableManager _tableManager;
        private readonly IDataOperations _dataOperations;

        private Store()
        {
            _databaseManager = new DatabaseManager();
            _tableManager = new TableManager();
            _dataOperations = new DataOperations();
        }

        public static Store GetInstance()
        {
            lock (_lock)
            {
                if (instance == null)
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        // Métodos que delegan a las implementaciones
        public bool DatabaseExists(string databaseName)
        {
            return _databaseManager.DatabaseExists(databaseName);
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            return _databaseManager.CreateDatabase(databaseName);
        }

        public OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions)
        {
            return _tableManager.CreateTable(databaseName, tableName, columnDefinitions);
        }

        public (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName)
        {
            return _tableManager.GetTableDefinition(databaseName, tableName);
        }

        public OperationStatus InsertRow(string databaseName, string tableName, string[] columns, string[] rowValues)
        {
            return _dataOperations.InsertRow(databaseName, tableName, columns, rowValues);
        }

        public List<Dictionary<string, string>> SelectAll(string databaseName, string tableName)
        {
            return _dataOperations.SelectAll(databaseName, tableName);
        }

        public bool IsTableEmpty(string databaseName, string tableName)
        {
            return _tableManager.IsTableEmpty(databaseName, tableName);
        }

        public OperationStatus DropTable(string databaseName, string tableName)
        {
            return _tableManager.DropTable(databaseName, tableName);
        }

        // Nuevo método para registrar un índice
        public OperationStatus RegisterIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            return _tableManager.CreateIndex(databaseName, tableName, columnName, indexName, indexType);
        }

        // Llamada a la búsqueda utilizando índice
        public List<Dictionary<string, string>> SearchUsingIndex(string databaseName, string tableName, string columnName, string searchValue)
        {
            return _tableManager.SearchUsingIndex(databaseName, tableName, columnName, searchValue);
        }

        // Llamada a la búsqueda secuencial
        public List<Dictionary<string, string>> SearchSequentially(string databaseName, string tableName, string columnName, string searchValue)
        {
            return _tableManager.SearchSequentially(databaseName, tableName, columnName, searchValue);
        }

        // Obtener información del índice
        public string GetIndexInfo(string databaseName, string tableName, string columnName)
        {
            return _tableManager.GetIndexInfo(databaseName, tableName, columnName);
        }

        // Eliminar filas específicas
        public OperationStatus DeleteRows(string databaseName, string tableName, string columnName, string value)
        {
            return _tableManager.DeleteRows(databaseName, tableName, columnName, value);
        }

        // Eliminar todas las filas
        public OperationStatus DeleteAllRows(string databaseName, string tableName)
        {
            return _tableManager.DeleteAllRows(databaseName, tableName);
        }
    }
}
