using System;
using System.Linq;
using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class CreateTable : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            int startIndex = query.IndexOf('(');
            int endIndex = query.LastIndexOf(')');

            if (startIndex > -1 && endIndex > startIndex)
            {
                var tableDefinition = query.Substring(0, startIndex).Trim();
                var columnsDefinition = query.Substring(startIndex + 1, endIndex - startIndex - 1).Trim();

                var tableParts = tableDefinition.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

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

                    columnsDefinition = columnsDefinition.Replace("\n", " ").Replace("\r", " ").Trim();

                    // Separa las columnas solo por comas
                    var columns = columnsDefinition.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                    var columnDefinitions = columns.Select(col =>
                    {
                        var cleanCol = col.Trim();

                        var firstSpaceIndex = cleanCol.IndexOf(' ');
                        if (firstSpaceIndex > 0)
                        {
                            var columnName = cleanCol.Substring(0, firstSpaceIndex).Trim();
                            var dataType = cleanCol.Substring(firstSpaceIndex).Trim();

                            return (ColumnName: columnName, DataType: dataType);
                        }
                        else
                        {
                            throw new Exception($"Definición de columna inválida: {col}");
                        }
                    }).ToArray();

                    var status = Store.GetInstance().CreateTable(databaseName, tableName, columnDefinitions);
                    if (status != OperationStatus.Success)
                    {
                        Console.WriteLine("Error al crear la tabla.");
                        return OperationStatus.Error;
                    }
                    else
                    {
                        Console.WriteLine($"Tabla '{tableName}' creada exitosamente en la base de datos '{databaseName}'.\n");
                        return OperationStatus.Success;
                    }
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para CREATE TABLE.");
                    return OperationStatus.Error;
                }
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para CREATE TABLE.");
                return OperationStatus.Error;
            }
        }
    }
}
