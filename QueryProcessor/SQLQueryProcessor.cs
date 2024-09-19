using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

                if (trimmedQuery.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    var sentenceParts = trimmedQuery.Split(' ');
                    if (sentenceParts.Length >= 5)
                    {
                        var databaseName = sentenceParts[2].Split('.')[0];
                        var tableName = sentenceParts[2].Split('.')[1];

                        var columns = sentenceParts.Skip(3).ToArray();  // Extraer definiciones de las columnas
                        var status = new CreateTable().Execute(databaseName, tableName, columns);  // Pasar columnas a Execute

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

                if (trimmedQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    if (parts.Length >= 5)
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
