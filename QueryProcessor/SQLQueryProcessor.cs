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

                // Procesar CREATE TABLE
                if (trimmedQuery.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    // Encontrar la posición de las columnas
                    int startIndex = trimmedQuery.IndexOf('(');
                    int endIndex = trimmedQuery.LastIndexOf(')');

                    if (startIndex > -1 && endIndex > startIndex)
                    {
                        var tableDefinition = trimmedQuery.Substring(0, startIndex).Trim();
                        var columnsDefinition = trimmedQuery.Substring(startIndex + 1, endIndex - startIndex - 1).Trim();

                        var tableParts = tableDefinition.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        if (tableParts.Length == 3)
                        {
                            var databaseName = tableParts[2].Split('.')[0];
                            var tableName = tableParts[2].Split('.')[1];

                            // Separar las columnas por coma
                            var columns = columnsDefinition.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                            // Ejecutar la creación de la tabla
                            var status = new CreateTable().Execute(databaseName, tableName, columns);
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
                    else
                    {
                        Console.WriteLine("Sintaxis incorrecta: falta la definición de columnas para CREATE TABLE.");
                        return OperationStatus.Error;
                    }
                }

                // Procesar SELECT
                if (trimmedQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    if (parts.Length >= 4)
                    {
                        var databaseName = parts[3];
                        var tableName = parts[4];
                        var status = Store.GetInstance().Select(databaseName, tableName);
                        if (status != OperationStatus.Success)
                        {
                            Console.WriteLine("Error al realizar SELECT.");
                            return OperationStatus.Error;
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
