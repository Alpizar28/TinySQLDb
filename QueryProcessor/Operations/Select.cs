using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Select : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            var selectParts = query.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (selectParts.Length >= 4 && selectParts[1] == "*" && selectParts[2].Equals("FROM", StringComparison.OrdinalIgnoreCase))
            {
                string databaseName = null;
                string tableName = null;

                var fullTableName = selectParts[3].Trim();

                fullTableName = fullTableName.TrimEnd(';');

                if (fullTableName.Contains('.'))
                {
                    var nameParts = fullTableName.Split('.');
                    databaseName = nameParts[0];
                    tableName = nameParts[1];
                }
                else
                {
                    tableName = fullTableName;
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

                Console.WriteLine($"Procesando SELECT de la tabla '{tableName}' en la base de datos '{databaseName}'");

                var results = Store.GetInstance().SelectAll(databaseName, tableName);
                if (results != null && results.Count > 0)
                {
                    var columnNames = results.First().Keys.ToList();

                    var columnWidths = new Dictionary<string, int>();

                    foreach (var columnName in columnNames)
                    {
                        int maxWidth = columnName.Length;

                        foreach (var row in results)
                        {
                            var valueLength = row[columnName].Length;
                            if (valueLength > maxWidth)
                                maxWidth = valueLength;
                        }

                        columnWidths[columnName] = maxWidth;
                    }

                    foreach (var columnName in columnNames)
                    {
                        Console.Write(columnName.PadRight(columnWidths[columnName] + 2));
                    }
                    Console.WriteLine();

                    Console.WriteLine(new string('-', columnWidths.Values.Sum() + (columnWidths.Count * 2)));

                    foreach (var row in results)
                    {
                        foreach (var columnName in columnNames)
                        {
                            Console.Write(row[columnName].PadRight(columnWidths[columnName] + 2));
                        }
                        Console.WriteLine();
                    }
                }
                else
                {
                    Console.WriteLine("No se encontraron registros.");
                }

                return OperationStatus.Success;
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para SELECT.");
                return OperationStatus.Error;
            }
        }
    }
}
