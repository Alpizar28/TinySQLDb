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

        public bool IsTableEmpty(string databaseName, string tableName)
        {
            return _tableManager.IsTableEmpty(databaseName, tableName);
        }

        // Nuevo método para eliminar una tabla
        public OperationStatus DropTable(string databaseName, string tableName)
        {
            return _tableManager.DropTable(databaseName, tableName);
        }
        public OperationStatus RegisterIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
{
    try
    {
        // Definir la ruta del archivo de índices
        string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
        string indexFilePath = Path.Combine(systemCatalogPath, "SystemIndexes.table");

        // Asegurarse de que el directorio existe
        Directory.CreateDirectory(systemCatalogPath);

        // Verificar si el índice ya existe
        if (File.Exists(indexFilePath))
        {
            var existingIndexes = File.ReadAllLines(indexFilePath);
            string indexEntry = $"{databaseName}|{tableName}|{columnName}|{indexName}|{indexType}";

            // Si el índice ya está registrado, no lo volvemos a registrar
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


        public OperationStatus RegisterDatabase(string databaseName)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string databasesFilePath = Path.Combine(systemCatalogPath, "SystemDatabases.table");

                Directory.CreateDirectory(systemCatalogPath);

                // Verificar si la base de datos ya está registrada
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

        // **Nuevo método para registrar una tabla en el System Catalog**
        public OperationStatus RegisterTable(string databaseName, string tableName)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string tablesFilePath = Path.Combine(systemCatalogPath, "SystemTables.table");

                Directory.CreateDirectory(systemCatalogPath);

                // Formato: <BaseDeDatos>|<NombreDeTabla>
                string tableInfo = $"{databaseName}|{tableName}";

                // Verificar si la tabla ya está registrada
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

        // **Nuevo método para registrar las columnas de una tabla en el System Catalog**
        public OperationStatus RegisterColumns(string databaseName, string tableName, (string ColumnName, string DataType)[] columns)
        {
            try
            {
                string systemCatalogPath = Path.Combine(@"C:\TinySql\Data\SystemCatalog");
                string columnsFilePath = Path.Combine(systemCatalogPath, "SystemColumns.table");

                Directory.CreateDirectory(systemCatalogPath);

                foreach (var column in columns)
                {
                    // Formato: <BaseDeDatos>|<NombreDeTabla>|<NombreDeColumna>|<TipoDeDato>
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


    }
}
