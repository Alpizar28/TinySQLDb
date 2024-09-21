using Entities;
using System.IO;
using StoreDataManager.Interfaces;

namespace StoreDataManager.Implementations
{
    public class TableManager : ITableManager
    {
        private const string DatabaseBasePath = @"C:\TinySql\Data\";

        public OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (File.Exists(tablePath))
                {
                    return OperationStatus.Warning;
                }

                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(columnDefinitions.Length);

                    foreach (var column in columnDefinitions)
                    {
                        writer.Write(column.ColumnName);
                        writer.Write(column.DataType);
                    }
                }

                return OperationStatus.Success;
            }
            catch
            {
                return OperationStatus.Error;
            }
        }

        public (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return null;
                }

                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int columnCount = reader.ReadInt32();
                    var columnDefinitions = new (string ColumnName, string DataType)[columnCount];

                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columnDefinitions[i] = (columnName, dataType);
                    }

                    return columnDefinitions;
                }
            }
            catch
            {
                return null;
            }
        }

        // Nuevo método para verificar si una tabla está vacía
        public bool IsTableEmpty(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return true; // Si no existe, lo consideramos vacío
                }

                using (FileStream stream = File.OpenRead(tablePath))
                {
                    return stream.Length <= 128; // Supongamos que las definiciones de columnas no ocupan más de 128 bytes
                }
            }
            catch
            {
                return false;
            }
        }

        // Nuevo método para eliminar una tabla
        public OperationStatus DropTable(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (File.Exists(tablePath))
                {
                    File.Delete(tablePath);
                    return OperationStatus.Success;
                }
                else
                {
                    return OperationStatus.Error; // No existe la tabla
                }
            }
            catch
            {
                return OperationStatus.Error;
            }
        }

        public OperationStatus CreateIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            try
            {
                // Verificar que el tipo de índice es válido
                if (indexType != "BTREE" && indexType != "BST")
                {
                    return OperationStatus.Error;
                }

                // Verificar que la tabla y la columna existen
                var columnDefinitions = GetTableDefinition(databaseName, tableName);
                if (columnDefinitions == null)
                {
                    Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                    return OperationStatus.Error;
                }

                bool columnExists = false;
                foreach (var column in columnDefinitions)
                {
                    if (column.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists)
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                    return OperationStatus.Error;
                }

                // Verificar que no haya valores repetidos en la columna
                var dataOperations = new DataOperations(); // Asegúrate de tener una instancia de IDataOperations
                var rows = dataOperations.SelectAll(databaseName, tableName);

                var uniqueValues = new HashSet<string>();
                foreach (var row in rows)
                {
                    if (!uniqueValues.Add(row[columnName]))
                    {
                        Console.WriteLine($"No se puede crear el índice. La columna '{columnName}' tiene valores repetidos.");
                        return OperationStatus.Error;
                    }
                }

                // Crear el índice en memoria (simulado)
                // Aquí puedes implementar la estructura real del índice según el tipo (BTREE o BST)

                // Registrar el índice en el System Catalog
                RegisterIndex(databaseName, tableName, columnName, indexName, indexType);

                Console.WriteLine($"Índice '{indexName}' creado exitosamente en la tabla '{tableName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void RegisterIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            // Implementar el registro del índice en el System Catalog
            string systemCatalogPath = Path.Combine(DatabaseBasePath, "SystemCatalog");
            Directory.CreateDirectory(systemCatalogPath);
            string indicesFilePath = Path.Combine(systemCatalogPath, "indices.txt");

            string indexInfo = $"{databaseName}|{tableName}|{columnName}|{indexName}|{indexType}";
            File.AppendAllText(indicesFilePath, indexInfo + Environment.NewLine);
        }
    }
}
