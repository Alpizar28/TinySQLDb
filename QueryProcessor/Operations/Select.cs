using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public OperationStatus Execute(string databaseName, string tableName)
        {
            var results = Store.GetInstance().SelectAll(databaseName, tableName);

            if (results == null)
            {
                Console.WriteLine("Error al obtener los registros.");
                return OperationStatus.Error;
            }

            if (results.Count == 0)
            {
                Console.WriteLine("No se encontraron registros.");
                return OperationStatus.Warning;
            }

            // Mostrar los resultados
            foreach (var row in results)
            {
                foreach (var column in row)
                {
                    Console.Write($"{column.Key}: {column.Value}\t");
                }
                Console.WriteLine();
            }

            return OperationStatus.Success;
        }
    }
}
