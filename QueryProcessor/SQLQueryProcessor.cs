using System;
using System.Collections.Generic;
using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        private Dictionary<string, ISqlOperation> operations;

        public SQLQueryProcessor()
        {
            operations = new Dictionary<string, ISqlOperation>(StringComparer.OrdinalIgnoreCase)
            {
                { "CREATE DATABASE", new CreateDatabase() },
                { "CREATE TABLE", new CreateTable() },
                { "INSERT INTO", new Insert() },
                { "SELECT", new Select() },
                { "DROP TABLE", new DropTable() },
                { "CREATE INDEX", new CreateIndex() }  // Agregamos CreateIndex al diccionario

            };
        }

        public OperationStatus Execute(string script)
        {
            var queries = script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            string currentDatabaseName = null;

            foreach (var query in queries)
            {
                var trimmedQuery = query.Trim();

                if (string.IsNullOrWhiteSpace(trimmedQuery)) continue;

                trimmedQuery = trimmedQuery.TrimEnd(';');

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

                var operationKey = GetOperationKey(trimmedQuery);
                if (operations.ContainsKey(operationKey))
                {
                    var status = operations[operationKey].Execute(trimmedQuery, ref currentDatabaseName);
                    if (status != OperationStatus.Success)
                    {
                        return status;
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

        private string GetOperationKey(string query)
        {
            foreach (var key in operations.Keys)
            {
                if (query.StartsWith(key, StringComparison.OrdinalIgnoreCase))
                {
                    return key;
                }
            }
            return string.Empty;
        }
    }
}
