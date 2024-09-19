using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class CreateTable
    {
        public OperationStatus Execute(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions)
        {
            try
            {
                // Lógica para crear la tabla
                var tablePath = $@"C:\TinySql\Data\{databaseName}\{tableName}.Table";

                // Verificar si la tabla ya existe
                if (File.Exists(tablePath))
                {
                    Console.WriteLine($"La tabla '{tableName}' ya existe en la base de datos '{databaseName}'.");
                    return OperationStatus.Warning;
                }

                // Crear la tabla y escribir las definiciones de las columnas
                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Escribir la cantidad de columnas
                    writer.Write(columnDefinitions.Length);

                    // Escribir las definiciones de las columnas (nombre y tipo de dato)
                    foreach (var column in columnDefinitions)
                    {
                        writer.Write(column.ColumnName);  // Escribir el nombre de la columna
                        writer.Write(column.DataType);    // Escribir el tipo de dato de la columna
                    }
                }

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en la base de datos '{databaseName}'.");
                return OperationStatus.Success;  // Retorna éxito si se creó correctamente
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la tabla '{tableName}' en la base de datos '{databaseName}': {ex.Message}");
                return OperationStatus.Error;  // Retorna error en caso de excepción
            }
        }
    }

}
