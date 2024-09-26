using System;
using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class CreateDatabase : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            var parts = query.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 3)
            {
                var databaseName = parts[2];

                if (Store.GetInstance().DatabaseExists(databaseName))
                {
                    Console.WriteLine($"La base de datos '{databaseName}' ya existe.");
                    return OperationStatus.Warning;
                }

                var status = Store.GetInstance().CreateDatabase(databaseName);
                if (status != OperationStatus.Success)
                {
                    Store.GetInstance().RegisterDatabase(databaseName);
                    Console.WriteLine("Error al crear la base de datos.");
                    return OperationStatus.Error;
                }
                else
                {
                    Console.WriteLine($"Base de datos '{databaseName}' creada exitosamente.");
                    return OperationStatus.Success;
                }
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para CREATE DATABASE.");
                return OperationStatus.Error;
            }
        }
    }
}
