using System;
using StoreDataManager;
using Entities;
using StoreDataManager.Interfaces;

namespace QueryProcessor.Operations
{
    public class CreateIndex : ISqlOperation
    {
        public OperationStatus Execute(string query, ref string currentDatabaseName)
        {
            // Verificar si hay una base de datos actual
            if (string.IsNullOrEmpty(currentDatabaseName))
            {
                Console.WriteLine("Error: No se ha seleccionado una base de datos.");
                return OperationStatus.Error;
            }

            // Parsear la consulta
            var parts = query.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length >= 8 && parts[0].Equals("CREATE", StringComparison.OrdinalIgnoreCase) && parts[1].Equals("INDEX", StringComparison.OrdinalIgnoreCase))
            {
                string indexName = parts[2];
                if (!parts[3].Equals("ON", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Sintaxis incorrecta para CREATE INDEX.");
                    return OperationStatus.Error;
                }

                // Obtener el nombre de la tabla y la columna
                string tableAndColumn = parts[4];
                int startIndex = tableAndColumn.IndexOf('(');
                int endIndex = tableAndColumn.IndexOf(')');

                if (startIndex == -1 || endIndex == -1 || endIndex <= startIndex)
                {
                    Console.WriteLine("Sintaxis incorrecta para especificar la columna en CREATE INDEX.");
                    return OperationStatus.Error;
                }

                string tableName = tableAndColumn.Substring(0, startIndex);
                string columnName = tableAndColumn.Substring(startIndex + 1, endIndex - startIndex - 1);

                if (!parts[5].Equals("OF", StringComparison.OrdinalIgnoreCase) || !parts[6].Equals("TYPE", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Sintaxis incorrecta para CREATE INDEX.");
                    return OperationStatus.Error;
                }

                string indexType = parts[7];

                // Crear el índice usando BTREE si se especifica este tipo
                if (indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
                {
                    BTree btree = new BTree(degree: 3);  // El grado es un parámetro del BTREE

                    // Aquí debes obtener los valores de la columna 'columnName' de la tabla 'tableName'
                    // e insertarlos en el BTree. Esto es solo un ejemplo de inserción:
                    // btree.Insert(valor);

                    // Registrar el índice en el catálogo del sistema
                    var store = Store.GetInstance();
                    store.RegisterIndex(currentDatabaseName, tableName, columnName, indexName, indexType);
                }

                Console.WriteLine($"Índice '{indexName}' creado exitosamente en la tabla '{tableName}' sobre la columna '{columnName}' con tipo '{indexType}'.");
                return OperationStatus.Success;
            }
            else
            {
                Console.WriteLine("Sintaxis incorrecta para CREATE INDEX.");
                return OperationStatus.Error;
            }
        }
    }
}
