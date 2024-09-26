using StoreDataManager;
using Entities;

namespace QueryProcessor.Operations
{
    public class Insert : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            // Separar la consulta entre la parte de la tabla y los valores
            var insertParts = query.Split(new[] { "VALUES" }, StringSplitOptions.RemoveEmptyEntries);

            if (insertParts.Length != 2)
            {
                Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                return OperationStatus.Error;
            }

            // Procesar la parte de la tabla (INSERT INTO <table-name>)
            var tableClause = insertParts[0].Trim();

            // Procesar la parte despues del VALUE
            var valuesClause = insertParts[1].Trim();

            // Dividir la parte de la tabla en palabras separadas
            var tableParts = tableClause.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (tableParts.Length < 3)
            {
                Console.WriteLine("Sintaxis incorrecta para INSERT INTO.");
                return OperationStatus.Error;
            }

            string databaseName = null;
            string tableName = null;

            // Obtener el nombre completo de la tabla
            var fullTableName = tableParts[2];

            // Verifica si el nombre de la tabla incluye el nombre de la base de datos ("miBase.tabla")
            if (fullTableName.Contains('.'))
            {
                var nameParts = fullTableName.Split('.');

                databaseName = nameParts[0]; 
                tableName = nameParts[1];
            }
            else
            {
                // Si no se especifica la base de datos, usar la base de datos actual
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
             //Limpia los valores
            valuesClause = valuesClause.Trim('(', ')', ';');

            var rawValues = valuesClause.Split(',');

            var values = new List<string>();
            foreach (var rawValue in rawValues)
            {
                string cleanedValue = rawValue.Trim();
                cleanedValue = cleanedValue.Trim('\'');
                values.Add(cleanedValue);           
            }

            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);

            if (tableDefinition == null)
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            if (!ValidateDataTypes(tableDefinition, values.ToArray()))
            {
                Console.WriteLine("Error: Los valores no corresponden a los tipos de datos esperados.");
                return OperationStatus.Error;
            }

            // Extraer los nombres de las columnas de la tabla
            var columns = tableDefinition.Select(c => c.ColumnName).ToArray();

            var status = Store.GetInstance().InsertRow(databaseName, tableName, columns, values.ToArray());

            if (status != OperationStatus.Success)
            {
                Console.WriteLine("Error al insertar en la tabla.");
                return OperationStatus.Error;
            }

            Console.WriteLine($"Registro insertado exitosamente en la tabla '{tableName}'.");
            return OperationStatus.Success;
        }

        // Método que valida que los tipos de datos de los valores coincidan con los tipos de columnas
        private bool ValidateDataTypes((string ColumnName, string DataType)[] columnDefinitions, string[] values)
        {
            if (columnDefinitions.Length != values.Length)
            {
                return false;
            }

            for (int i = 0; i < columnDefinitions.Length; i++)
            {
                string dataType = columnDefinitions[i].DataType.ToUpper();
                string value = values[i];

                if (dataType.StartsWith("VARCHAR"))
                {
                    continue;
                }
                else if (dataType == "INTEGER")
                {
                    if (!int.TryParse(value, out _))
                    {
                        return false;
                    }
                }
                else if (dataType == "DATETIME")
                {
                    if (!DateTime.TryParse(value, out _))
                    {
                        return false; 
                    }
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
