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

        // Método para registrar un índice
        public OperationStatus RegisterIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string indexFilePath = Path.Combine(systemCatalogPath, "SystemIndexes.table");

                Directory.CreateDirectory(systemCatalogPath);

                if (File.Exists(indexFilePath))
                {
                    var existingIndexes = File.ReadAllLines(indexFilePath);
                    string indexEntry = $"{databaseName}|{tableName}|{columnName}|{indexName}|{indexType}";

                    if (existingIndexes.Contains(indexEntry))
                    {
                        Console.WriteLine($"El índice '{indexName}' ya está registrado en la tabla '{tableName}' sobre la columna '{columnName}'.");
                        return OperationStatus.Warning;
                    }
                }

                // Registrar el índice si no existe
                string newIndexInfo = $"{databaseName}|{tableName}|{columnName}|{indexName}|{indexType}";
                File.AppendAllText(indexFilePath, newIndexInfo + Environment.NewLine);

                Console.WriteLine($"Índice '{indexName}' registrado exitosamente en la tabla '{tableName}' sobre la columna '{columnName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al registrar el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Método para registrar una base de datos
        public OperationStatus RegisterDatabase(string databaseName)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string databasesFilePath = Path.Combine(systemCatalogPath, "SystemDatabases.table");

                Directory.CreateDirectory(systemCatalogPath);

                var existingDatabases = File.ReadAllLines(databasesFilePath);
                if (existingDatabases.Contains(databaseName))
                {
                    return OperationStatus.Warning;  // Ya existe
                }

                // Añadir la base de datos al archivo SystemDatabases
                File.AppendAllText(databasesFilePath, databaseName + Environment.NewLine);

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al registrar la base de datos: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Método para registrar una tabla en el System Catalog
        public OperationStatus RegisterTable(string databaseName, string tableName)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string tablesFilePath = Path.Combine(systemCatalogPath, "SystemTables.table");

                Directory.CreateDirectory(systemCatalogPath);

                string tableInfo = $"{databaseName}|{tableName}";

                var existingTables = File.ReadAllLines(tablesFilePath);
                if (existingTables.Contains(tableInfo))
                {
                    return OperationStatus.Warning;  // Ya existe
                }

                // Añadir la tabla al archivo SystemTables
                File.AppendAllText(tablesFilePath, tableInfo + Environment.NewLine);

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al registrar la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Método para registrar las columnas de una tabla en el System Catalog
        public OperationStatus RegisterColumns(string databaseName, string tableName, (string ColumnName, string DataType)[] columns)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string columnsFilePath = Path.Combine(systemCatalogPath, "SystemColumns.table");

                Directory.CreateDirectory(systemCatalogPath);

                foreach (var column in columns)
                {
                    string columnInfo = $"{databaseName}|{tableName}|{column.ColumnName}|{column.DataType}";

                    // Añadir la columna al archivo SystemColumns
                    File.AppendAllText(columnsFilePath, columnInfo + Environment.NewLine);
                }

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al registrar las columnas: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Búsqueda utilizando índice
        public List<Dictionary<string, string>> SearchUsingIndex(string databaseName, string tableName, string columnName, string searchValue)
        {
            return _tableManager.SearchUsingIndex(databaseName, tableName, columnName, searchValue);
        }

        // Búsqueda secuencial
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
