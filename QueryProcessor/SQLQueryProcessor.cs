using System;
using System.Linq;
using QueryProcessor.Exceptions;
using StoreDataManager;
using Entities;
using System.Collections.Generic;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string script)
        {
            var queries = script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            string currentDatabaseName = null;

            foreach (var query in queries)
            {
                var trimmedQuery = query.Trim();

                if (string.IsNullOrWhiteSpace(trimmedQuery)) continue;

                // Eliminar el punto y coma al final de la consulta
                trimmedQuery = trimmedQuery.TrimEnd(';');

                // Procesar USE
                if (trimmedQuery.StartsWith("USE", StringComparison.OrdinalIgnoreCase))
                {
                    var parts = trimmedQuery.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length == 2)
                    {
                        currentDatabaseName = parts[1];
                        Console.WriteLine($"Base de datos actual establecida a '{currentDatabaseName}'.");
                        continue;
                    }
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta para USE.");
                        return OperationStatus.Error;
                    }
                }

                // Procesar CREATE DATABASE
                if (trimmedQuery.StartsWith("CREATE DATABASE", StringComparison.OrdinalIgnoreCase))
                {
                    var parts = trimmedQuery.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length == 3)
                    {
                        var databaseName = parts[2];

                        if (Store.GetInstance().DatabaseExists(databaseName))
                        {
                            Console.WriteLine($"La base de datos '{databaseName}' ya existe.");
                            continue;
                        }

                        var status = Store.GetInstance().CreateDatabase(databaseName);
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
                else if (trimmedQuery.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    int startIndex = trimmedQuery.IndexOf('(');
                    int endIndex = trimmedQuery.LastIndexOf(')');

                    if (startIndex > -1 && endIndex > startIndex)
                    {
                        var tableDefinition = trimmedQuery.Substring(0, startIndex).Trim();
                        var columnsDefinition = trimmedQuery.Substring(startIndex + 1, endIndex - startIndex - 1).Trim();

                        var tableParts = tableDefinition.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                        if (tableParts.Length >= 3)
                        {
                            string databaseName = null;
                            string tableName = null;

                            // Soporta 'CREATE TABLE BaseDeDatos.Tabla' o 'CREATE TABLE Tabla'
                            var fullTableName = tableParts[2];
                            if (fullTableName.Contains('.'))
                            {
                                var nameParts = fullTableName.Split('.');
                                databaseName = nameParts[0];
                                tableName = nameParts[1];
                            }
                            else
                            {
                                tableName = fullTableName;
                                if (!string.IsNullOrEmpty(currentDatabaseName))
                                {
                                    databaseName = currentDatabaseName;
                                }
                                else
                                {
                                    Console.WriteLine("Error: No se especificó la base de datos y no hay una base de datos actual establecida.");
                                    return OperationStatus.Error;
                                }
                            }

                            // Actualización en el análisis de las columnas
                            var columns = columnsDefinition.Split(new[] { ',', '\n' }, StringSplitOptions.RemoveEmptyEntries);

                            var columnDefinitions = columns.Select(col =>
                            {
                                var cleanCol = col.Trim();

                                var firstSpaceIndex = cleanCol.IndexOf(' ');
                                if (firstSpaceIndex > 0)
                                {
                                    var columnName = cleanCol.Substring(0, firstSpaceIndex).Trim();
                                    var dataType = cleanCol.Substring(firstSpaceIndex).Trim();

                                    return (ColumnName: columnName, DataType: dataType);
                                }
                                else
                                {
                                    throw new Exception($"Definición de columna inválida: {col}");
                                }
                            }).ToArray();

                            var status = Store.GetInstance().CreateTable(databaseName, tableName, columnDefinitions);
                            if (status != OperationStatus.Success)
                            {
                                Console.WriteLine("Error al crear la tabla.");
                                return OperationStatus.Error;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Sintaxis incorrecta para CREATE TABLE.");
                            return OperationStatus.Error;
                        }
                    }
                }
                else if (trimmedQuery.StartsWith("INSERT INTO", StringComparison.OrdinalIgnoreCase))
                {
                    var insertParts = trimmedQuery.Split(new[] { "VALUES" }, StringSplitOptions.RemoveEmptyEntries);
                    if (insertParts.Length == 2)
                    {
                        var tableClause = insertParts[0].Trim();  // Parte 'INSERT INTO ...'
                        var valuesClause = insertParts[1].Trim();  // Parte '(...);'

                        var tableParts = tableClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        if (tableParts.Length >= 3)
                        {
                            string databaseName = null;
                            string tableName = null;

                            // Soporta 'INSERT INTO BaseDeDatos.Tabla' o 'INSERT INTO Tabla'
                            var fullTableName = tableParts[2];
                            if (fullTableName.Contains('.'))
                            {
                                var nameParts = fullTableName.Split('.');
                                databaseName = nameParts[0];
                                tableName = nameParts[1];
                            }
                            else
                            {
                                tableName = fullTableName;
                                if (!string.IsNullOrEmpty(currentDatabaseName))
                                {
                                    databaseName = currentDatabaseName;
                                }
                                else
                                {
                                    Console.WriteLine("Error: No se especificó la base de datos y no hay una base de datos actual establecida.");
                                    return OperationStatus.Error;
                                }
                            }

                            var values = valuesClause.Trim('(', ')', ';')
                                .Split(',')
                                .Select(v => v.Trim().Trim('\''))
                                .ToArray();

                            // Obtener la definición de la tabla
                            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
                            if (tableDefinition == null)
                            {
                                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                                return OperationStatus.Error;
                            }

                            // Validar los tipos de datos
                            if (!ValidateDataTypes(tableDefinition, values))
                            {
                                Console.WriteLine("Error: Los valores no corresponden a los tipos de datos esperados.");
                                return OperationStatus.Error;
                            }

                            // Insertar la fila
                            var columns = tableDefinition.Select(c => c.ColumnName).ToArray();
                            var status = Store.GetInstance().InsertRow(databaseName, tableName, columns, values);
                            if (status != OperationStatus.Success)
                            {
                                Console.WriteLine("Error al insertar en la tabla.");
                                return OperationStatus.Error;
                            }
                        }
                        else
                        {
                            Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                            return OperationStatus.Error;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                        return OperationStatus.Error;
                    }
                }
                // Procesar SELECT
                else if (trimmedQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    var selectParts = trimmedQuery.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (selectParts.Length >= 4 && selectParts[1] == "*" && selectParts[2].Equals("FROM", StringComparison.OrdinalIgnoreCase))
                    {
                        string databaseName = null;
                        string tableName = null;

                        var fullTableName = selectParts[3].Trim();

                        // Eliminar el punto y coma al final del nombre de la tabla
                        fullTableName = fullTableName.TrimEnd(';');

                        if (fullTableName.Contains('.'))
                        {
                            var nameParts = fullTableName.Split('.');
                            databaseName = nameParts[0];
                            tableName = nameParts[1];
                        }
                        else
                        {
                            tableName = fullTableName;
                            if (!string.IsNullOrEmpty(currentDatabaseName))
                            {
                                databaseName = currentDatabaseName;
                            }
                            else
                            {
                                Console.WriteLine("Error: No se especificó la base de datos y no hay una base de datos actual establecida.");
                                return OperationStatus.Error;
                            }
                        }

                        Console.WriteLine($"Procesando SELECT de la tabla '{tableName}' en la base de datos '{databaseName}'");

                        var results = Store.GetInstance().SelectAll(databaseName, tableName);
                        if (results != null && results.Count > 0)
                        {
                            // Obtener la lista de columnas
                            var columnNames = results.First().Keys.ToList();

                            // Calcular el ancho máximo para cada columna
                            var columnWidths = new Dictionary<string, int>();

                            foreach (var columnName in columnNames)
                            {
                                int maxWidth = columnName.Length;

                                foreach (var row in results)
                                {
                                    var valueLength = row[columnName].Length;
                                    if (valueLength > maxWidth)
                                        maxWidth = valueLength;
                                }

                                columnWidths[columnName] = maxWidth;
                            }

                            // Imprimir los encabezados de las columnas
                            foreach (var columnName in columnNames)
                            {
                                Console.Write(columnName.PadRight(columnWidths[columnName] + 2));
                            }
                            Console.WriteLine();

                            // Imprimir una línea separadora
                            Console.WriteLine(new string('-', columnWidths.Values.Sum() + (columnWidths.Count * 2)));

                            // Imprimir las filas
                            foreach (var row in results)
                            {
                                foreach (var columnName in columnNames)
                                {
                                    Console.Write(row[columnName].PadRight(columnWidths[columnName] + 2));
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
                        Console.WriteLine("Sintaxis incorrecta para SELECT.");
                        return OperationStatus.Error;
                    }
                }
                else
                {
                    Console.WriteLine($"Comando no reconocido: {trimmedQuery}");
                    return OperationStatus.Error;
                }
            }

            return OperationStatus.Success;
        }

        // Validar que los valores coincidan con los tipos de datos de la tabla
        private static bool ValidateDataTypes((string ColumnName, string DataType)[] columnDefinitions, string[] values)
        {
            if (columnDefinitions.Length != values.Length)
            {
                return false;
            }

            for (int i = 0; i < columnDefinitions.Length; i++)
            {
                var dataType = columnDefinitions[i].DataType.ToUpper();
                var value = values[i];

                if (dataType.StartsWith("VARCHAR"))
                {
                    // Opcional: validar longitud si está especificada
                    continue;
                }
                else if (dataType == "INTEGER")
                {
                    if (!int.TryParse(value, out _))
                        return false;
                }
                else if (dataType == "DATETIME")
                {
                    if (!DateTime.TryParse(value, out _))
                        return false;
                }
                else
                {
                    Console.WriteLine($"Tipo de dato '{dataType}' no soportado.");
                    return false;
                }
            }

            return true;
        }
    }
}
