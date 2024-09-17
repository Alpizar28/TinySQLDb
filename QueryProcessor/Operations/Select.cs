using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public OperationStatus Execute(string databaseName, string tableName)
        {
            // Llamar a la instancia de Store y seleccionar la tabla
            return Store.GetInstance().Select(databaseName, tableName);
        }
    }
}
