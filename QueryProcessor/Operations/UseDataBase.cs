using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class UseDatabase : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            var parts = query.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length >= 2)
            {
                var databaseName = parts[1];

                if (Store.GetInstance().DatabaseExists(databaseName))
                {
                    currentDatabaseName = databaseName;
                    Console.WriteLine($"Base de datos actual establecida a '{databaseName}'.");
                    return OperationStatus.Success;
                }
                else
                {
                    Console.WriteLine($"La base de datos '{databaseName}' no existe.");
                    return OperationStatus.Error;
                }
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para USE.");
                return OperationStatus.Error;
            }
        }
    }
}
