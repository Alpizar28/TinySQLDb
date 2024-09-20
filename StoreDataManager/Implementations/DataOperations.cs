using Entities;
using System.Collections.Generic;
using System.IO;
using StoreDataManager.Interfaces;

namespace StoreDataManager.Implementations
{
    public class DataOperations : IDataOperations
    {
        private const string DatabaseBasePath = @"C:\TinySql\Data\";

        public OperationStatus InsertRow(string databaseName, string tableName, string[] columns, string[] rowValues)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return OperationStatus.Error;
                }

                using (FileStream stream = new FileStream(tablePath, FileMode.Append))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write((byte)1);

                    foreach (var value in rowValues)
                    {
                        writer.Write(value);
                    }
                }

                return OperationStatus.Success;
            }
            catch
            {
                return OperationStatus.Error;
            }
        }

        public List<Dictionary<string, string>> SelectAll(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return null;
                }

                var results = new List<Dictionary<string, string>>();

                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int columnCount = reader.ReadInt32();
                    var columns = new List<string>();

                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columns.Add(columnName);
                    }

                    while (stream.Position < stream.Length)
                    {
                        byte rowMarker = reader.ReadByte();
                        if (rowMarker != 1)
                        {
                            return null;
                        }

                        var row = new Dictionary<string, string>();

                        foreach (var column in columns)
                        {
                            string value = reader.ReadString();
                            row[column] = value;
                        }

                        results.Add(row);
                    }
                }

                return results;
            }
            catch
            {
                return null;
            }
        }
    }
}
