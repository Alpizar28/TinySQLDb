using Entities;

namespace StoreDataManager.Interfaces
{
    public interface IDatabaseManager
    {
        bool DatabaseExists(string databaseName);
        OperationStatus CreateDatabase(string databaseName);
    }
}
