using Entities;
using System.IO;
using StoreDataManager.Interfaces;

namespace StoreDataManager.Implementations
{
    public class DatabaseManager : IDatabaseManager
    {
        private const string DatabaseBasePath = @"C:\TinySql\Data\";

        public DatabaseManager()
        {
            Directory.CreateDirectory(DatabaseBasePath);
        }

        public bool DatabaseExists(string databaseName)
        {
            var databasePath = Path.Combine(DatabaseBasePath, databaseName);
            return Directory.Exists(databasePath);
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            try
            {
                var databasePath = Path.Combine(DatabaseBasePath, databaseName);

                if (Directory.Exists(databasePath))
                {
                    // Puedes manejar este caso según lo necesites
                    return OperationStatus.Warning;
                }

                Directory.CreateDirectory(databasePath);
                return OperationStatus.Success;
            }
            catch
            {
                return OperationStatus.Error;
            }
        }
    }
}
