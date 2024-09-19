using System;
using System.Linq;
using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string script)
        {
            // Dividir el script en múltiples sentencias SQL usando el delimitador ;
            var queries = script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var query in queries)
            {
                var trimmedQuery = query.Trim();

                if (string.IsNullOrWhiteSpace(trimmedQuery)) continue;

                var parts = trimmedQuery.Split(' ');

                // Procesar CREATE DATABASE
                if (trimmedQuery.StartsWith("CREATE DATABASE", StringComparison.OrdinalIgnoreCase))
                {
                    if (parts.Length == 3)
                    {
                        var databaseName = parts[2];

                        // Verificar si la base de datos ya existe
                        if (Store.GetInstance().DatabaseExists(databaseName))
                        {
                            Console.WriteLine($"La base de datos '{databaseName}' ya existe.");
                            return OperationStatus.Success; // Evitar que lance un error
                        }

                        var status = new CreateDataBase().Execute(databaseName);
                        if (status != OperationStatus.Success)
                        {
                            Console.WriteLine("Error al crear la base de datos.");
                            return OperationStatus.Error;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta para CREATE DATABASE.");
                        return OperationStatus.Error;
                    }
                }

                if (trimmedQuery.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    // Encontrar la posición de las columnas
                    int startIndex = trimmedQuery.IndexOf('(');
                    int endIndex = trimmedQuery.LastIndexOf(')');

                    if (startIndex > -1 && endIndex > startIndex)
                    {
                        // Obtener la definición de la tabla antes del paréntesis
                        var tableDefinition = trimmedQuery.Substring(0, startIndex).Trim();
                        var columnsDefinition = trimmedQuery.Substring(startIndex + 1, endIndex - startIndex - 1).Trim();

                        // Dividir la parte anterior al paréntesis para extraer base de datos y nombre de tabla
                        var tableParts = tableDefinition.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                        if (tableParts.Length == 4)
                        {
                            var databaseName = tableParts[2];  // Obtener el nombre de la base de datos
                            var tableName = tableParts[3];     // Obtener el nombre de la tabla

                            // Separar las columnas por comas
                            var columns = columnsDefinition.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            // Procesar cada columna (nombre y tipo de dato)
                            var columnDefinitions = columns.Select(col =>
                            {
                                var parts = col.Trim().Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                                if (parts.Length == 2)
                                {
                                    return (ColumnName: parts[0], DataType: parts[1]);
                                }
                                else
                                {
                                    throw new Exception($"Definición de columna inválida: {col}");
                                }
                            }).ToArray();

                            // Lógica para crear la tabla (base de datos, nombre de tabla y definiciones de columnas)
                            var status = new CreateTable().Execute(databaseName, tableName, columnDefinitions);
                            if (status != OperationStatus.Success)
                            {
                                Console.WriteLine("Error al crear la tabla.");
                                return OperationStatus.Error;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Sintaxis incorrecta para CREATE TABLE. Se esperaba 'CREATE TABLE [Database] [Table]'.");
                            return OperationStatus.Error;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta: falta la definición de columnas para CREATE TABLE.");
                        return OperationStatus.Error;
                    }
                }

                if (trimmedQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    // Dividimos la consulta en partes.
                    var selectParts = trimmedQuery.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                    if (selectParts.Length >= 4 && selectParts[2].ToUpper() == "FROM")
                    {
                        var columns = selectParts[1].Split(',');  // Columnas solicitadas
                        var databaseName = selectParts[3];
                        var tableName = selectParts[4];

                        if (columns.Length == 1 && columns[0] == "*")
                        {
                            // Si se selecciona "*", seleccionamos todas las columnas.
                            var results = Store.GetInstance().SelectAll(databaseName, tableName);
                            if (results != null)
                            {
                                foreach (var row in results)
                                {
                                    foreach (var column in row)
                                    {
                                        Console.Write($"{column.Key}: {column.Value} ");
                                    }
                                    Console.WriteLine();
                                }
                            }
                            else
                            {
                                Console.WriteLine("No se encontraron registros.");
                            }
                        }
                        else
                        {
                            // Selecciona columnas específicas
                            var results = Store.GetInstance().SelectColumns(databaseName, tableName, columns);
                            if (results != null)
                            {
                                foreach (var row in results)
                                {
                                    foreach (var column in columns)
                                    {
                                        if (row.ContainsKey(column))
                                        {
                                            Console.Write($"{column}: {row[column]} ");
                                        }
                                    }
                                    Console.WriteLine();
                                }
                            }
                            else
                            {
                                Console.WriteLine("No se encontraron registros o columnas.");
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta para SELECT.");
                        return OperationStatus.Error;
                    }
                }




            }

            return OperationStatus.Success;
        }
    }
}
