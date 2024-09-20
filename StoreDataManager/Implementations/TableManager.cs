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
    }
}
