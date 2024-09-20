using System;
using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class DropTable : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            var parts = query.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length == 3 && parts[0].Equals("DROP", StringComparison.OrdinalIgnoreCase) && parts[1].Equals("TABLE", StringComparison.OrdinalIgnoreCase))
            {
                string databaseName = null;
                string tableName = parts[2];

                if (tableName.Contains('.'))
                {
                    var nameParts = tableName.Split('.');
                    databaseName = nameParts[0];
                    tableName = nameParts[1];
                }
                else
                {
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

                // Verificar si la tabla está vacía antes de eliminarla
                var store = Store.GetInstance();
                if (store.IsTableEmpty(databaseName, tableName))
                {
                    var status = store.DropTable(databaseName, tableName);
                    if (status == OperationStatus.Success)
                    {
                        Console.WriteLine($"Tabla '{tableName}' eliminada exitosamente de la base de datos '{databaseName}'.");
                        return OperationStatus.Success;
                    }
                    else
                    {
                        Console.WriteLine($"Error al eliminar la tabla '{tableName}' en la base de datos '{databaseName}'.");
                        return OperationStatus.Error;
                    }
                }
                else
                {
                    Console.WriteLine($"No se puede eliminar la tabla '{tableName}' porque no está vacía.");
                    return OperationStatus.Error;
                }
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para DROP TABLE.");
                return OperationStatus.Error;
            }
        }
    }
}
