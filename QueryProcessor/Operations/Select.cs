using System;
using System.Collections.Generic;
using System.Linq;
using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Select : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            if (string.IsNullOrEmpty(currentDatabaseName))
            {
                Console.WriteLine("No hay una base de datos seleccionada. Use USE <database_name> para seleccionar una.");
                return OperationStatus.Error;
            }

            // Eliminar punto y coma al final si existe
            query = query.Trim().TrimEnd(';');

            // Separar la cláusula SELECT del resto
            var selectIndex = query.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
            var fromIndex = query.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);

            if (selectIndex == -1 || fromIndex == -1)
            {
                Console.WriteLine("Sintaxis incorrecta. Falta SELECT o FROM.");
                return OperationStatus.Error;
            }

            var selectClause = query.Substring(selectIndex + 6, fromIndex - (selectIndex + 6)).Trim();
            var remainingQuery = query.Substring(fromIndex + 4).Trim();

            // Procesar la cláusula FROM y posibles cláusulas WHERE
            string tableName = null;
            string whereClause = null;

            var whereIndex = remainingQuery.IndexOf("WHERE", StringComparison.OrdinalIgnoreCase);

            if (whereIndex != -1)
            {
                tableName = remainingQuery.Substring(0, whereIndex).Trim();
                whereClause = remainingQuery.Substring(whereIndex + 5).Trim();
            }
            else
            {
                tableName = remainingQuery.Trim();
            }

            // Determinar la base de datos y el nombre de la tabla
            string databaseName = currentDatabaseName;

            if (tableName.Contains('.'))
            {
                var nameParts = tableName.Split('.');
                databaseName = nameParts[0];
                tableName = nameParts[1];
            }

            // Obtener la definición de la tabla
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            if (tableDefinition == null)
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            // Obtener los datos de la tabla
            var rows = Store.GetInstance().SelectAll(databaseName, tableName);
            if (rows == null)
            {
                Console.WriteLine($"Error al obtener los datos de la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            // Filtrar las filas si hay cláusula WHERE
            if (!string.IsNullOrEmpty(whereClause))
            {
                rows = ApplyWhereClause(rows, whereClause, tableDefinition);
                if (rows == null)
                {
                    return OperationStatus.Error;
                }
            }

            // Seleccionar las columnas especificadas
            List<string> selectedColumns;
            if (selectClause.Trim() == "*")
            {
                selectedColumns = tableDefinition.Select(c => c.ColumnName).ToList();
            }
            else
            {
                selectedColumns = selectClause.Split(',')
                                              .Select(c => c.Trim())
                                              .ToList();

                // Verificar que las columnas existan en la tabla
                foreach (var column in selectedColumns)
                {
                    if (!tableDefinition.Any(c => c.ColumnName.Equals(column, StringComparison.OrdinalIgnoreCase)))
                    {
                        Console.WriteLine($"La columna '{column}' no existe en la tabla '{tableName}'.");
                        return OperationStatus.Error;
                    }
                }
            }

            // Mostrar los resultados
            DisplayResults(selectedColumns, rows);

            return OperationStatus.Success;
        }

        private List<Dictionary<string, string>> ApplyWhereClause(List<Dictionary<string, string>> rows, string whereClause, (string ColumnName, string DataType)[] tableDefinition)
        {
            // Parsear la cláusula WHERE
            var operators = new[] { ">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT" };
            string selectedOperator = operators.FirstOrDefault(op => whereClause.IndexOf(op, StringComparison.OrdinalIgnoreCase) != -1);

            if (string.IsNullOrEmpty(selectedOperator))
            {
                Console.WriteLine("Operador de comparación no soportado en la cláusula WHERE.");
                return null;
            }

            var parts = whereClause.Split(new[] { selectedOperator }, StringSplitOptions.None);
            if (parts.Length != 2)
            {
                Console.WriteLine("Sintaxis incorrecta en la cláusula WHERE.");
                return null;
            }

            var columnName = parts[0].Trim();
            var value = parts[1].Trim().Trim('\'');

            // Obtener el tipo de dato de la columna
            var columnDefinition = tableDefinition.FirstOrDefault(c => c.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnDefinition.ColumnName == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe.");
                return null;
            }

            // Filtrar las filas según la condición
            var filteredRows = rows.Where(row =>
            {
                if (!row.ContainsKey(columnName))
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en los datos.");
                    return false;
                }

                var cellValue = row[columnName];

                switch (selectedOperator.ToUpper())
                {
                    case "=":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a == b);
                    case "!=":
                    case "<>":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a != b);
                    case ">":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a > b);
                    case "<":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a < b);
                    case ">=":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a >= b);
                    case "<=":
                        return CompareValues(cellValue, value, columnDefinition.DataType, (a, b) => a <= b);
                    case "LIKE":
                        return cellValue.Contains(value);
                    case "NOT":
                        return !cellValue.Contains(value);
                    default:
                        Console.WriteLine($"Operador '{selectedOperator}' no soportado.");
                        return false;
                }
            }).ToList();

            return filteredRows;
        }

        private bool CompareValues(string cellValue, string value, string dataType, Func<dynamic, dynamic, bool> comparison)
        {
            try
            {
                switch (dataType.ToUpper())
                {
                    case "INTEGER":
                        int intCellValue = int.Parse(cellValue);
                        int intValue = int.Parse(value);
                        return comparison(intCellValue, intValue);
                    case "DATETIME":
                        DateTime dateCellValue = DateTime.Parse(cellValue);
                        DateTime dateValue = DateTime.Parse(value);
                        return comparison(dateCellValue, dateValue);
                    case string s when s.StartsWith("VARCHAR"):
                        return comparison(cellValue, value);
                    default:
                        Console.WriteLine($"Tipo de dato '{dataType}' no soportado para comparación.");
                        return false;
                }
            }
            catch
            {
                Console.WriteLine($"Error al comparar valores: '{cellValue}' y '{value}' como '{dataType}'.");
                return false;
            }
        }

        private void DisplayResults(List<string> columns, List<Dictionary<string, string>> rows)
        {
            // Mostrar los nombres de las columnas
            Console.WriteLine(string.Join("\t", columns));

            // Mostrar separador
            Console.WriteLine(new string('-', columns.Sum(c => c.Length + 8)));

            // Mostrar las filas
            foreach (var row in rows)
            {
                var values = columns.Select(col => row.ContainsKey(col) ? row[col] : "").ToArray();
                Console.WriteLine(string.Join("\t", values));
            }
        }
    }
}
