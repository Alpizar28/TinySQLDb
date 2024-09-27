using System;
using System.Collections.Generic;
using System.Linq;
using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Update : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            // Dividimos la consulta en la parte SET y la parte WHERE
            var updateParts = query.Split(new[] { "SET", "WHERE" }, StringSplitOptions.RemoveEmptyEntries).Select(part => part.Trim()).ToArray();

            if (updateParts.Length < 2)
            {
                Console.WriteLine("Sintaxis incorrecta para UPDATE.");
                return OperationStatus.Error;
            }

            string tableClause = updateParts[0].Trim();
            string setClause = updateParts[1].Trim();
            string whereClause = updateParts.Length > 2 ? updateParts[2].Trim() : null;

            // Procesar el nombre de la tabla
            var tableParts = tableClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (tableParts.Length < 2 || !tableParts[0].Equals("UPDATE", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Sintaxis incorrecta para UPDATE.");
                return OperationStatus.Error;
            }

            string tableName = tableParts[1];
            string databaseName = currentDatabaseName;

            // Obtener la definición de la tabla
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            if (tableDefinition == null)
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            // Procesar la cláusula SET
            var setParts = setClause.Split('=');
            if (setParts.Length != 2)
            {
                Console.WriteLine("Sintaxis incorrecta en la cláusula SET.");
                return OperationStatus.Error;
            }

            string columnToUpdate = setParts[0].Trim();
            string newValue = setParts[1].Trim().Trim('\'');

            // Validar si la columna existe
            if (!tableDefinition.Any(c => c.ColumnName.Equals(columnToUpdate, StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine($"La columna '{columnToUpdate}' no existe en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            // Si no se especifica WHERE, actualizamos todas las filas
            if (string.IsNullOrEmpty(whereClause))
            {
                var status = Store.GetInstance().UpdateAllRows(databaseName, tableName, columnToUpdate, newValue);
                if (status == OperationStatus.Success)
                {
                    Console.WriteLine($"Todas las filas de la tabla '{tableName}' fueron actualizadas.");
                }
                return status;
            }

            // Procesar la cláusula WHERE
            var condition = ParseWhereClause(whereClause);

            // Verificar si hay un índice en la columna especificada en el WHERE
            var indexInfo = Store.GetInstance().GetIndexInfo(databaseName, tableName, condition.ColumnName);
            List<Dictionary<string, string>> rowsToUpdate;

            if (indexInfo != null)
            {
                Console.WriteLine($"Índice encontrado para la columna '{condition.ColumnName}'.");
                rowsToUpdate = Store.GetInstance().SearchUsingIndex(databaseName, tableName, condition.ColumnName, condition.Value.Trim('\'').ToLower());
            }
            else
            {
                Console.WriteLine($"No se encontró índice para la columna '{condition.ColumnName}', búsqueda secuencial.");
                rowsToUpdate = Store.GetInstance().SearchSequentially(databaseName, tableName, condition.ColumnName.ToLower(), condition.Value.Trim('\'').ToLower());
            }

            if (rowsToUpdate == null || rowsToUpdate.Count == 0)
            {
                Console.WriteLine("No se encontraron filas que coincidan con la condición.");
                return OperationStatus.Error;
            }

            var updateStatus = Store.GetInstance().UpdateRows(databaseName, tableName, columnToUpdate, newValue, condition.ColumnName, condition.Value.Trim('\'').ToLower());
            if (updateStatus == OperationStatus.Success)
            {
                Console.WriteLine($"Filas actualizadas en la tabla '{tableName}' que coinciden con la condición.");
            }

            return updateStatus;
        }

        // Método para analizar la cláusula WHERE y obtener la columna, el operador y el valor
        private (string ColumnName, string Operator, string Value) ParseWhereClause(string whereClause)
        {
            var operators = new[] { ">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT" };
            string selectedOperator = operators.FirstOrDefault(op => whereClause.Contains(op));

            if (string.IsNullOrEmpty(selectedOperator))
            {
                throw new Exception("Operador de comparación no soportado en la cláusula WHERE.");
            }

            var parts = whereClause.Split(new[] { selectedOperator }, 2, StringSplitOptions.None);

            if (parts.Length != 2)
            {
                throw new Exception("Sintaxis incorrecta en la cláusula WHERE.");
            }

            string columnName = parts[0].Trim();
            string value = parts[1].Trim();

            return (columnName, selectedOperator, value);
        }
    }
}
