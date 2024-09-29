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
            // Check if the current database is selected
            if (string.IsNullOrEmpty(currentDatabaseName))
            {
                Console.WriteLine("No hay una base de datos seleccionada. Use USE <database_name> para seleccionar una.");
                return OperationStatus.Error;
            }

            // Remove the semicolon at the end of the query if it exists
            query = query.Trim();
            query = query.TrimEnd(';');

            // Find the index of SELECT and FROM in the query
            int selectIndex = query.IndexOf("SELECT", StringComparison.OrdinalIgnoreCase);
            int fromIndex = query.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);

            // Check if both SELECT and FROM exist in the query
            if (selectIndex == -1 || fromIndex == -1)
            {
                Console.WriteLine("Sintaxis incorrecta. Falta SELECT o FROM.");
                return OperationStatus.Error;
            }

            // Extract the SELECT clause
            string selectClause = query.Substring(selectIndex + 6, fromIndex - (selectIndex + 6));
            selectClause = selectClause.Trim();

            // Extract the remaining part of the query after the FROM clause
            string remainingQuery = query.Substring(fromIndex + 4).Trim();

            // Initialize variables for table name, WHERE clause, and ORDER BY clause
            string tableName = null;
            string whereClause = null;
            string orderByClause = null;
            bool orderDescending = false;

            // Look for WHERE and ORDER BY clauses
            int whereIndex = remainingQuery.IndexOf("WHERE", StringComparison.OrdinalIgnoreCase);
            int orderByIndex = remainingQuery.IndexOf("ORDER BY", StringComparison.OrdinalIgnoreCase);

            if (orderByIndex != -1)
            {
                // Extract the ORDER BY clause
                orderByClause = remainingQuery.Substring(orderByIndex + 8).Trim();

                // Adjust the remaining query by removing the ORDER BY clause
                remainingQuery = remainingQuery.Substring(0, orderByIndex).Trim();

                // Check if the ORDER BY clause specifies DESC or ASC
                if (orderByClause.EndsWith("DESC", StringComparison.OrdinalIgnoreCase))
                {
                    orderByClause = orderByClause.Substring(0, orderByClause.Length - 4).Trim();
                    orderDescending = true;
                }
                else if (orderByClause.EndsWith("ASC", StringComparison.OrdinalIgnoreCase))
                {
                    orderByClause = orderByClause.Substring(0, orderByClause.Length - 3).Trim();
                }
            }

            // Extract the WHERE clause if it exists
            if (whereIndex != -1)
            {
                tableName = remainingQuery.Substring(0, whereIndex).Trim();
                whereClause = remainingQuery.Substring(whereIndex + 5).Trim();
            }
            else
            {
                tableName = remainingQuery.Trim();
            }

            // Determine the database and table name
            string databaseName = currentDatabaseName;

            if (tableName.Contains('.'))
            {
                string[] nameParts = tableName.Split('.');
                databaseName = nameParts[0];
                tableName = nameParts[1];
            }

            // Get the table definition
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            if (tableDefinition == null)
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            // Get all the data from the table
            var rows = Store.GetInstance().SelectAll(databaseName, tableName);
            if (rows == null)
            {
                Console.WriteLine($"Error al obtener los datos de la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            // Filter the rows based on the WHERE clause if it exists
            if (!string.IsNullOrEmpty(whereClause))
            {
                rows = ApplyWhereClause(rows, whereClause, tableDefinition);
                if (rows == null)
                {
                    return OperationStatus.Error;
                }
            }

            // Process the SELECT clause (select specific columns or all)
            List<string> selectedColumns = new List<string>();
            if (selectClause.Trim() == "*")
            {
                // Select all columns
                foreach (var column in tableDefinition)
                {
                    selectedColumns.Add(column.ColumnName);
                }
            }
            else
            {
                // Select specific columns
                string[] columnsArray = selectClause.Split(',');
                foreach (string column in columnsArray)
                {
                    string trimmedColumn = column.Trim();
                    selectedColumns.Add(trimmedColumn);
                }

                // Verify that the columns exist in the table definition
                foreach (var column in selectedColumns)
                {
                    bool columnExists = tableDefinition.Any(c => c.ColumnName.Equals(column, StringComparison.OrdinalIgnoreCase));
                    if (!columnExists)
                    {
                        Console.WriteLine($"La columna '{column}' no existe en la tabla '{tableName}'.");
                        return OperationStatus.Error;
                    }
                }
            }

            // Sort the rows if ORDER BY is specified
            if (!string.IsNullOrEmpty(orderByClause))
            {
                rows = Quicksort(rows, orderByClause, orderDescending);
            }

            // Display the results
            DisplayResults(selectedColumns, rows);

            return OperationStatus.Success;
        }

        private List<Dictionary<string, string>> ApplyWhereClause(List<Dictionary<string, string>> rows, string whereClause, (string ColumnName, string DataType)[] tableDefinition)
        {
            // List of supported operators
            string[] operators = { ">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT" };
            string selectedOperator = operators.FirstOrDefault(op => whereClause.IndexOf(op, StringComparison.OrdinalIgnoreCase) != -1);

            if (string.IsNullOrEmpty(selectedOperator))
            {
                Console.WriteLine("Operador de comparación no soportado en la cláusula WHERE.");
                return null;
            }

            // Split the WHERE clause into column name and value
            string[] parts = whereClause.Split(new[] { selectedOperator }, StringSplitOptions.None);
            if (parts.Length != 2)
            {
                Console.WriteLine("Sintaxis incorrecta en la cláusula WHERE.");
                return null;
            }

            string columnName = parts[0].Trim();
            string value = parts[1].Trim().Trim('\'');

            // Get the column definition from the table
            var columnDefinition = tableDefinition.FirstOrDefault(c => c.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (columnDefinition.ColumnName == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe.");
                return null;
            }

            // Filter rows based on the condition in the WHERE clause
            List<Dictionary<string, string>> filteredRows = new List<Dictionary<string, string>>();
            foreach (var row in rows)
            {
                if (!row.ContainsKey(columnName))
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en los datos.");
                    return null;
                }

                string cellValue = row[columnName];
                bool comparisonResult = CompareValues(cellValue, value, columnDefinition.DataType, selectedOperator);

                if (comparisonResult)
                {
                    filteredRows.Add(row);
                }
            }

            return filteredRows;
        }

        private bool CompareValues(string cellValue, string value, string dataType, string selectedOperator)
        {
            // Conversion and comparison based on data type
            try
            {
                if (dataType.ToUpper() == "INTEGER")
                {
                    int intCellValue = int.Parse(cellValue);
                    int intValue = int.Parse(value);
                    return EvaluateComparison(intCellValue, intValue, selectedOperator);
                }
                else if (dataType.ToUpper() == "DATETIME")
                {
                    DateTime dateCellValue = DateTime.Parse(cellValue);
                    DateTime dateValue = DateTime.Parse(value);
                    return EvaluateComparison(dateCellValue, dateValue, selectedOperator);
                }
                else if (dataType.ToUpper().StartsWith("VARCHAR"))
                {
                    if (selectedOperator.ToUpper() == "LIKE")
                    {
                        return cellValue.ToLower().Contains(value.ToLower().Replace("%", ""));
                    }
                    return EvaluateComparison(cellValue, value, selectedOperator);
                }
                else
                {
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

        private bool EvaluateComparison<T>(T cellValue, T value, string selectedOperator) where T : IComparable
        {
            // Handle all possible comparison operators
            switch (selectedOperator)
            {
                case "=":
                    return cellValue.CompareTo(value) == 0;
                case "!=":
                case "<>":
                    return cellValue.CompareTo(value) != 0;
                case ">":
                    return cellValue.CompareTo(value) > 0;
                case "<":
                    return cellValue.CompareTo(value) < 0;
                case ">=":
                    return cellValue.CompareTo(value) >= 0;
                case "<=":
                    return cellValue.CompareTo(value) <= 0;
                default:
                    Console.WriteLine($"Operador '{selectedOperator}' no soportado.");
                    return false;
            }
        }

        private List<Dictionary<string, string>> Quicksort(List<Dictionary<string, string>> rows, string column, bool descending)
        {
            // Base case: if there is 1 or fewer rows, no sorting is needed
            if (rows.Count <= 1)
            {
                return rows;
            }

            // Select the pivot (middle element)
            var pivot = rows[rows.Count / 2][column];

            // Partition the rows into three groups: less than, equal to, and greater than the pivot
            var less = new List<Dictionary<string, string>>();
            var greater = new List<Dictionary<string, string>>();
            var equal = new List<Dictionary<string, string>>();

            foreach (var row in rows)
            {
                var cellValue = row[column];

                if (string.Compare(cellValue, pivot) < 0)
                {
                    less.Add(row); // Values less than the pivot
                }
                else if (string.Compare(cellValue, pivot) > 0)
                {
                    greater.Add(row); // Values greater than the pivot
                }
                else
                {
                    equal.Add(row); // Values equal to the pivot
                }
            }

            // Recursively sort the less and greater lists
            var sortedLess = Quicksort(less, column, descending);
            var sortedGreater = Quicksort(greater, column, descending);

            // Combine the sorted lists
            var result = new List<Dictionary<string, string>>();

            if (descending)
            {
                result.AddRange(sortedGreater);
                result.AddRange(equal);
                result.AddRange(sortedLess);
            }
            else
            {
                result.AddRange(sortedLess);
                result.AddRange(equal);
                result.AddRange(sortedGreater);
            }

            return result;
        }

        private void DisplayResults(List<string> columns, List<Dictionary<string, string>> rows)
        {
            // Definir anchos fijos para las columnas para una alineación correcta
            int columnWidth = 15;

            // Mostrar los nombres de las columnas con un ancho fijo
            foreach (var column in columns)
            {
                Console.Write($"{column.PadRight(columnWidth)}\t");
            }
            Console.WriteLine();

            // Mostrar un separador
            Console.WriteLine(new string('-', columns.Count * (columnWidth + 4)));

            // Mostrar los valores de cada fila
            foreach (var row in rows)
            {
                foreach (var column in columns)
                {
                    if (row.ContainsKey(column))
                    {
                        Console.Write($"{row[column].PadRight(columnWidth)}\t");
                    }
                    else
                    {
                        Console.Write(new string(' ', columnWidth) + "\t"); // Asegura que la columna vacía tenga el mismo ancho
                    }
                }
                Console.WriteLine();
            }
        }


    }
}
