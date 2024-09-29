using System;
using System.Collections.Generic;
using System.Diagnostics; // Para Stopwatch
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
                { "USE", new UseDatabase() },
                { "DELETE", new Delete() },
                { "UPDATE", new Update() }

            };
        }

        public OperationStatus Execute(string script)
        {
            if (string.IsNullOrWhiteSpace(script))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("El script SQL está vacío.");
                Console.ResetColor();
                return OperationStatus.Error;
            }

            var queries = script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            string currentDatabaseName = null;

            foreach (var query in queries)
            {
                var trimmedQuery = query.Trim();

                if (string.IsNullOrWhiteSpace(trimmedQuery)) continue;

                var operationKey = GetOperationKey(trimmedQuery);
                if (!string.IsNullOrEmpty(operationKey) && operations.ContainsKey(operationKey))
                {
                    try
                    {
                        Stopwatch stopwatch = new Stopwatch();
                        stopwatch.Start();

                        var status = operations[operationKey].Execute(trimmedQuery, ref currentDatabaseName);

                        stopwatch.Stop();
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine($"Operación '{operationKey}' ejecutada en {stopwatch.Elapsed.TotalMilliseconds:F4} ms.");
                        Console.WriteLine();
                        Console.ResetColor();

                        if (status != OperationStatus.Success)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine($"Error al ejecutar la operación: '{trimmedQuery}'");
                            Console.ResetColor();
                            return status;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Excepción al ejecutar la operación '{operationKey}': {ex.Message}");
                        Console.ResetColor();
                        return OperationStatus.Error;
                    }
                }
                else
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine($"Comando no reconocido o sintaxis incorrecta: '{trimmedQuery}'");
                    Console.ResetColor();
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
