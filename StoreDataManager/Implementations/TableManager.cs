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
    }
}
