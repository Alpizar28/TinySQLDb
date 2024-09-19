using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string databaseName, string tableName, string[] columnDefinitions)
        {
            var columnTuples = columnDefinitions.Select(col => (col, "STRING")).ToArray();  // Suponiendo que todas las columnas son de tipo "STRING"

            return Store.GetInstance().CreateTable(databaseName, tableName, columnTuples);
        }
    }

}
