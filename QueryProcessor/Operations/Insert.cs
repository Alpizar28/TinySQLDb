using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Insert : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            var insertParts = query.Split(new[] { "VALUES" }, StringSplitOptions.RemoveEmptyEntries);
            if (insertParts.Length == 2)
            {
                var tableClause = insertParts[0].Trim();
                var valuesClause = insertParts[1].Trim();

                var tableParts = tableClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (tableParts.Length >= 3)
                {
                    string databaseName = null;
                    string tableName = null;

                    var fullTableName = tableParts[2];
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

                    var values = valuesClause.Trim('(', ')', ';')
                        .Split(',')
                        .Select(v => v.Trim().Trim('\''))
                        .ToArray();

                    var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
                    if (tableDefinition == null)
                    {
                        Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                        return OperationStatus.Error;
                    }

                    if (!ValidateDataTypes(tableDefinition, values))
                    {
                        Console.WriteLine("Error: Los valores no corresponden a los tipos de datos esperados.");
                        return OperationStatus.Error;
                    }

                    var columns = tableDefinition.Select(c => c.ColumnName).ToArray();
                    var status = Store.GetInstance().InsertRow(databaseName, tableName, columns, values);
                    if (status != OperationStatus.Success)
                    {
                        Console.WriteLine("Error al insertar en la tabla.");
                        return OperationStatus.Error;
                    }
                    else
                    {
                        Console.WriteLine($"Registro insertado exitosamente en la tabla '{tableName}'.");
                        return OperationStatus.Success;
                    }
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                    return OperationStatus.Error;
                }
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                return OperationStatus.Error;
            }
            return OperationStatus.Success; ///quitar por aquello
        }

        private bool ValidateDataTypes((string ColumnName, string DataType)[] columnDefinitions, string[] values)
        {
            if (columnDefinitions.Length != values.Length)
            {
                return false;
            }

            for (int i = 0; i < columnDefinitions.Length; i++)
            {
                var dataType = columnDefinitions[i].DataType.ToUpper();
                var value = values[i];

                if (dataType.StartsWith("VARCHAR"))
                {
                    continue;
                }
                else if (dataType == "INTEGER")
                {
                    if (!int.TryParse(value, out _))
                        return false;
                }
                else if (dataType == "DATETIME")
                {
                    if (!DateTime.TryParse(value, out _))
                        return false;
                }
                else
                {
                    Console.WriteLine($"Tipo de dato '{dataType}' no soportado.");
                    return false;
                }
            }

            return true;
        }
    }
}
