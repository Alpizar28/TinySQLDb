using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateDataBase
    {
        internal OperationStatus Execute(string databaseName)
        {
            return Store.GetInstance().CreateDatabase(databaseName);
        }
    }
}
