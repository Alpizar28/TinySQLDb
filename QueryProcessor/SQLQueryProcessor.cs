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
                { "CREATE INDEX", new CreateIndex() },  
                { "USE", new UseDatabase() }  

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

                var operationKey = GetOperationKey(trimmedQuery);
                if (!string.IsNullOrEmpty(operationKey) && operations.ContainsKey(operationKey))
                {
                    var status = operations[operationKey].Execute(trimmedQuery, ref currentDatabaseName);
                    if (status != OperationStatus.Success)
                    {
                        Console.WriteLine($"Error al ejecutar la operación: {trimmedQuery}");
                        return status;
                    }
                }
                else
                {
                    Console.WriteLine($"Comando no reconocido o sintaxis incorrecta: {trimmedQuery}");
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
