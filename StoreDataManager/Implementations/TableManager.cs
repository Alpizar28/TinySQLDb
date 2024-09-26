using Entities;
using System.IO;
using StoreDataManager.Interfaces;
using System.Collections.Generic;
using System.Linq;

namespace StoreDataManager.Implementations
{
    public class TableManager : ITableManager
    {
        private const string DatabaseBasePath = @"C:\TinySql\Data\";

        public OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (File.Exists(tablePath))
                {
                    return OperationStatus.Warning;
                }

                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(columnDefinitions.Length);

                    foreach (var column in columnDefinitions)
                    {
                        writer.Write(column.ColumnName);
                        writer.Write(column.DataType);
                    }
                }

                return OperationStatus.Success;
            }
            catch
            {
                return OperationStatus.Error;
            }
        }

        public (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return null;
                }

                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    int columnCount = reader.ReadInt32(); // Leer el número de columnas
                    var columnDefinitions = new (string ColumnName, string DataType)[columnCount];

                    // Leer los nombres de las columnas y los tipos de datos
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columnDefinitions[i] = (columnName, dataType);
                    }

                    return columnDefinitions;
                }
            }
            catch
            {
                return null;
            }
        }

        public bool IsTableEmpty(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    return true; // Si no existe, lo consideramos vacío
                }

                using (FileStream stream = File.OpenRead(tablePath))
                {
                    return stream.Length <= 128; // Supongamos que las definiciones de columnas no ocupan más de 128 bytes
                }
            }
            catch
            {
                return false;
            }
        }

        public OperationStatus DropTable(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (File.Exists(tablePath))
                {
                    File.Delete(tablePath);
                    return OperationStatus.Success;
                }
                else
                {
                    return OperationStatus.Error; // No existe la tabla
                }
            }
            catch
            {
                return OperationStatus.Error;
            }
        }

        public OperationStatus CreateIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            try
            {
                // Verificar que el tipo de índice es válido
                if (indexType != "BTREE" && indexType != "BST")
                {
                    return OperationStatus.Error;
                }

                // Verificar que la tabla y la columna existen
                var columnDefinitions = GetTableDefinition(databaseName, tableName);
                if (columnDefinitions == null)
                {
                    Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                    return OperationStatus.Error;
                }

                bool columnExists = false;
                foreach (var column in columnDefinitions)
                {
                    if (column.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        columnExists = true;
                        break;
                    }
                }

                if (!columnExists)
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                    return OperationStatus.Error;
                }

                // Verificar que no haya valores repetidos en la columna
                var rows = SelectAll(databaseName, tableName);
                if (rows == null) // Verificación añadida
                {
                    return OperationStatus.Error;
                }

                var uniqueValues = new HashSet<string>();
                foreach (var row in rows)
                {
                    if (!uniqueValues.Add(row[columnName]))
                    {
                        Console.WriteLine($"No se puede crear el índice. La columna '{columnName}' tiene valores repetidos.");
                        return OperationStatus.Error;
                    }
                }

                // Crear el índice en memoria (simulado)
                RegisterIndex(databaseName, tableName, columnName, indexName, indexType);

                Console.WriteLine($"Índice '{indexName}' creado exitosamente en la tabla '{tableName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void RegisterIndex(string databaseName, string tableName, string columnName, string indexName, string indexType)
        {
            // Implementar el registro del índice en el System Catalog
            string systemCatalogPath = Path.Combine(DatabaseBasePath, "SystemCatalog");
            Directory.CreateDirectory(systemCatalogPath);
            string indicesFilePath = Path.Combine(systemCatalogPath, "indices.txt");

            string indexInfo = $"{databaseName}|{tableName}|{columnName}|{indexName}|{indexType}";
            File.AppendAllText(indicesFilePath, indexInfo + Environment.NewLine);
        }

        public OperationStatus DeleteAllRows(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                    return OperationStatus.Error;
                }

                var tableDefinition = GetTableDefinition(databaseName, tableName);
                if (tableDefinition == null)
                {
                    Console.WriteLine($"No se pudo obtener la definición de la tabla '{tableName}'.");
                    return OperationStatus.Error;
                }

                // Reescribir el archivo con solo la definición de columnas (vacío de datos)
                using (var tempStream = new FileStream(tablePath, FileMode.Create))
                using (var writer = new BinaryWriter(tempStream))
                {
                    writer.Write(tableDefinition.Length); // Cantidad de columnas
                    foreach (var column in tableDefinition)
                    {
                        writer.Write(column.ColumnName);
                        writer.Write(column.DataType);
                    }
                }

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al eliminar todas las filas de la tabla '{tableName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }


        public List<Dictionary<string, string>> SearchUsingIndex(string databaseName, string tableName, string columnName, string searchValue)
        {
            var indexInfo = GetIndexInfo(databaseName, tableName, columnName);
            if (indexInfo == null)
            {
                Console.WriteLine($"No se encontró un índice para la columna '{columnName}' en la tabla '{tableName}'.");
                return null;
            }

            // Lógica para buscar utilizando el índice (simulada)
            return SearchSequentially(databaseName, tableName, columnName, searchValue);
        }


        public List<Dictionary<string, string>> SearchSequentially(string databaseName, string tableName, string columnName, string searchValue)
        {
            var allRows = SelectAll(databaseName, tableName);
            if (allRows == null)
            {
                Console.WriteLine($"No se encontraron filas en la tabla '{tableName}'.");
                return null;
            }

            // Obtener la definición de la columna para saber si es un número entero o cadena
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            var columnDefinition = tableDefinition?.FirstOrDefault(c => c.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));

            if (columnDefinition == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe.");
                return null;
            }

            var matchedRows = new List<Dictionary<string, string>>();

            // Normalizar el nombre de la columna a minúsculas
            string normalizedColumnName = columnName.ToLower();

            foreach (var row in allRows)
            {
                // Verificar si la fila contiene la columna, sin distinguir entre mayúsculas y minúsculas
                if (!row.Keys.Any(k => k.Equals(normalizedColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en la fila actual.");
                    continue;
                }

                // Buscar el valor en la columna insensible a mayúsculas
                string cellValue = row.FirstOrDefault(kvp => kvp.Key.Equals(normalizedColumnName, StringComparison.OrdinalIgnoreCase)).Value.Trim();
                bool isMatch = false;

                // Comparar según el tipo de dato
                if (columnDefinition?.DataType.ToUpper() == "INTEGER")
                {
                    // Convertir ambos valores a entero y compararlos
                    if (int.TryParse(cellValue, out int intCellValue) && int.TryParse(searchValue.Trim(), out int intSearchValue))
                    {
                        isMatch = intCellValue == intSearchValue;
                    }
                }
                else
                {
                    // Comparar cadenas ignorando mayúsculas
                    isMatch = cellValue.Equals(searchValue.Trim(), StringComparison.OrdinalIgnoreCase);
                }

                if (isMatch)
                {
                    matchedRows.Add(row);
                }
            }

            if (matchedRows.Count == 0)
            {
                Console.WriteLine($"No se encontraron coincidencias para {columnName} = {searchValue}");
            }

            return matchedRows;
        }




        public string GetIndexInfo(string databaseName, string tableName, string columnName)
        {
            string systemCatalogPath = Path.Combine(DatabaseBasePath, "SystemCatalog", "indices.txt");

            if (!File.Exists(systemCatalogPath))
            {
                return null;
            }

            var allLines = File.ReadAllLines(systemCatalogPath);
            foreach (var line in allLines)
            {
                var indexParts = line.Split('|');
                if (indexParts.Length == 5 && indexParts[0] == databaseName && indexParts[1] == tableName && indexParts[2] == columnName)
                {
                    return line; // Devuelve la información del índice
                }
            }

            return null;
        }

        public OperationStatus DeleteRows(string databaseName, string tableName, string columnName, string value)
        {
            var allRows = SelectAll(databaseName, tableName);
            if (allRows == null || allRows.Count == 0)
            {
                Console.WriteLine($"No se encontraron filas en la tabla '{tableName}' para eliminar.");
                return OperationStatus.Error;
            }

            // Obtener la definición de la columna para saber si es un número entero
            var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName);
            var columnDefinition = tableDefinition.FirstOrDefault(c => c.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));

            // Cambiar la comparación de tuplas con null
            if (columnDefinition == default || string.IsNullOrEmpty(columnDefinition.ColumnName))
            {
                Console.WriteLine($"La columna '{columnName}' no existe.");
                return OperationStatus.Error;
            }

            // Lista para almacenar las filas restantes (las que no se eliminarán)
            var remainingRows = new List<Dictionary<string, string>>();

            // Filtrar manualmente las filas
            foreach (var row in allRows)
            {
                if (!row.ContainsKey(columnName))
                {
                    Console.WriteLine($"La columna '{columnName}' no existe en los datos.");
                    remainingRows.Add(row); // Si la columna no existe, mantenemos la fila (caso raro)
                    continue;
                }

                // Obtener el valor de la fila
                string cellValue = row[columnName].Trim();
                bool isMatch = false;

                // Comparar según el tipo de dato
                if (columnDefinition.DataType.ToUpper() == "INTEGER")
                {
                    // Convertir ambos valores a entero y compararlos
                    if (int.TryParse(cellValue, out int intCellValue) && int.TryParse(value, out int intValue))
                    {
                        isMatch = intCellValue == intValue;
                    }
                    else
                    {
                        Console.WriteLine($"Error al convertir '{cellValue}' o '{value}' a entero.");
                    }
                }
                else
                {
                    // Comparación insensible a mayúsculas para otros tipos de datos
                    isMatch = cellValue.Equals(value.Trim(), StringComparison.OrdinalIgnoreCase);
                }

                if (!isMatch)
                {
                    // Mantener la fila si no coincide
                    remainingRows.Add(row);
                }
            }

            if (remainingRows.Count == allRows.Count)
            {
                return OperationStatus.Error;
            }

            // Escribir las filas restantes nuevamente en el archivo
            return WriteRowsToFile(databaseName, tableName, remainingRows);
        }

        private OperationStatus WriteRowsToFile(string databaseName, string tableName, List<Dictionary<string, string>> rows)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                // Usamos un archivo temporal para evitar problemas si algo falla
                var tempFilePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}_temp.table");

                var tableDefinition = Store.GetInstance().GetTableDefinition(databaseName, tableName); // Obtener definición de tabla

                using (var stream = new FileStream(tempFilePath, FileMode.Create))
                using (var writer = new BinaryWriter(stream))
                {
                    // Escribir la definición de la tabla
                    writer.Write(tableDefinition.Length);
                    foreach (var column in tableDefinition)
                    {
                        writer.Write(column.ColumnName);
                        writer.Write(column.DataType);
                    }

                    // Escribir las filas restantes
                    foreach (var row in rows)
                    {
                        writer.Write((byte)1); // Escribir marcador de fila válida
                        foreach (var value in row.Values)
                        {
                            writer.Write(value);
                        }
                    }
                }

                // Reemplazamos el archivo original solo si la escritura fue exitosa
                File.Replace(tempFilePath, tablePath, null);

                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al escribir filas en el archivo de la tabla '{tableName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }


        public List<Dictionary<string, string>> SelectAll(string databaseName, string tableName)
        {
            try
            {
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

                if (!File.Exists(tablePath))
                {
                    Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                    return null;
                }

                var results = new List<Dictionary<string, string>>();

                using (var stream = File.OpenRead(tablePath))
                using (var reader = new BinaryReader(stream))
                {
                    int columnCount = reader.ReadInt32(); // Leer el número de columnas
                    var columns = new List<string>();

                    // Leer los nombres de las columnas (ignoramos los tipos de datos)
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();  // Leer nombre de la columna
                        columns.Add(columnName);
                        string columnType = reader.ReadString();  // Leer tipo de la columna, pero no lo necesitamos aquí
                    }

                    // Leer cada fila
                    while (stream.Position < stream.Length)
                    {
                        byte rowMarker = reader.ReadByte(); // Marcador de fila (1 = válida, 0 = eliminada)

                        if (rowMarker == 1) // Solo procesar filas válidas
                        {
                            var row = new Dictionary<string, string>();

                            for (int i = 0; i < columnCount; i++)
                            {
                                string value = reader.ReadString();
                                row[columns[i]] = value;
                            }

                            results.Add(row);
                        }
                        else
                        {
                            Console.WriteLine("Fila eliminada encontrada y saltada.");
                        }
                    }
                }
                return results;
            }
            catch (EndOfStreamException eosEx)
            {
                Console.WriteLine($"Error en SelectAll: Se intentó leer más allá del final del archivo. {eosEx.Message}");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error en SelectAll: {ex.Message}");
                return null;
            }
        }




    }
}
