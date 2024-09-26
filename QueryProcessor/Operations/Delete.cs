using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Delete : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            // Dividimos la consulta para identificar la tabla y la condición WHERE
            var deleteParts = query.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
            string tableClause = deleteParts[0].Trim();
            string whereClause = deleteParts.Length > 1 ? deleteParts[1].Trim() : null;

            // Procesamos el nombre de la tabla
            var tableParts = tableClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
            if (tableParts.Length < 3 || !tableParts[0].Equals("DELETE", StringComparison.OrdinalIgnoreCase) || !tableParts[1].Equals("FROM", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Sintaxis incorrecta para DELETE.");
                return OperationStatus.Error;
            }

            string tableName = tableParts[2];
            string databaseName = currentDatabaseName;

            // Obtener la definición de la tabla
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            if (tableDefinition == null)
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            // Si no se especifica WHERE, eliminamos todas las filas
            if (whereClause == null)
            {
                var status = Store.GetInstance().DeleteAllRows(databaseName, tableName);
                if (status == OperationStatus.Success)
                {
                    Console.WriteLine($"Todas las filas eliminadas de la tabla '{tableName}'.");
                }
                return status;
            }

            // Si se especifica WHERE, debemos procesar la condición
            var condition = ParseWhereClause(whereClause);

            // Verificar si hay un índice en la columna especificada en el WHERE
            var indexInfo = Store.GetInstance().GetIndexInfo(databaseName, tableName, condition.ColumnName);
            List<Dictionary<string, string>> rowsToDelete;

            if (indexInfo != null)
            {
                rowsToDelete = Store.GetInstance().SearchUsingIndex(databaseName, tableName, condition.ColumnName, condition.Value.Trim('\'').Trim().ToLower());
            }
            else
            {
                rowsToDelete = Store.GetInstance().SearchSequentially(databaseName, tableName, condition.ColumnName.ToLower(), condition.Value.Trim('\'').Trim().ToLower());
            }

            if (rowsToDelete == null || rowsToDelete.Count == 0)
            {
                return OperationStatus.Error;
            }

            var deleteStatus = Store.GetInstance().DeleteRows(databaseName, tableName, condition.ColumnName, condition.Value.Trim('\'').Trim().ToLower());
            if (deleteStatus == OperationStatus.Success)
            {
                Console.WriteLine($"Filas eliminadas de la tabla '{tableName}' que coinciden con la condición.");
            }

            return deleteStatus;
        }

        // Método para analizar la cláusula WHERE y obtener la columna, el operador y el valor
        private (string ColumnName, string Operator, string Value) ParseWhereClause(string whereClause)
        {
            // Lista de operadores soportados
            var operators = new[] { ">=", "<=", "<>", "!=", "=", ">", "<", "LIKE", "NOT" };
            string selectedOperator = operators.FirstOrDefault(op => whereClause.IndexOf(op, StringComparison.OrdinalIgnoreCase) != -1);

            if (string.IsNullOrEmpty(selectedOperator))
            {
                throw new Exception("Operador de comparación no soportado en la cláusula WHERE.");
            }

            // Dividir la cláusula WHERE en nombre de columna y valor
            string[] parts = whereClause.Split(new[] { selectedOperator }, StringSplitOptions.None);
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
