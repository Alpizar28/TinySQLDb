using StoreDataManager.Interfaces;
using StoreDataManager.Implementations;
using Entities;
using System.Collections.Generic;

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
    }
}
