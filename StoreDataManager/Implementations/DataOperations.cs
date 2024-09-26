using StoreDataManager.Interfaces;
using Entities;
using System.Collections.Generic;
using System.IO;
using System;

public class DataOperations : IDataOperations
{
    private const string DatabaseBasePath = @"C:\TinySql\Data\";

    // Método para insertar filas
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
                writer.Write((byte)1); // Marcador para fila válida

                // Verificar que el número de columnas coincida con los valores
                if (columns.Length != rowValues.Length)
                {
                    return OperationStatus.Error;
                }

                // Escribir cada valor de la fila basado en su tipo
                for (int i = 0; i < rowValues.Length; i++)
                {
                    string value = rowValues[i];
                    writer.Write(value); // Por ahora escribe los valores como string
                }
            }

            return OperationStatus.Success;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error al insertar la fila: {ex.Message}");
            return OperationStatus.Error;
        }
    }

    // Método para seleccionar todas las filas de una tabla
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
                int columnCount;
                List<string> columns = new List<string>();

                // Leer el número de columnas y los nombres de las columnas
                try
                {
                    columnCount = reader.ReadInt32(); // Leer el número de columnas

                    // Leer los nombres de las columnas
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string columnType = reader.ReadString(); // Leer también el tipo de la columna pero no lo usaremos aquí
                        columns.Add(columnName);  // Solo añadimos el nombre de la columna
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error al leer la definición de la tabla: {e.Message}");
                    return null;
                }

                // Leer las filas de la tabla
                while (stream.Position < stream.Length)
                {
                    byte rowMarker = reader.ReadByte();
                    if (rowMarker != 1)
                    {
                        // Omitir filas no válidas o eliminadas
                        continue;
                    }

                    var row = new Dictionary<string, string>();

                    // Leer los valores de la fila
                    for (int i = 0; i < columnCount; i++)
                    {
                        string value = reader.ReadString();
                        row[columns[i]] = value;
                    }

                    results.Add(row);
                }
            }

            return results;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error en SelectAll: {ex.Message}");
            return null;
        }
    }
}
