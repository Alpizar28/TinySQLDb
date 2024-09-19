using Entities;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        public static Store GetInstance()
        {
            lock (_lock)
            {
                if (instance == null)
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog()
        {
            // Verificar que el sistema de archivos del catálogo esté creado
            Directory.CreateDirectory(SystemCatalogPath);
        }

        // Nuevo método para verificar si una base de datos ya existe
        public bool DatabaseExists(string databaseName)
        {
            var databasePath = $@"{DataPath}\{databaseName}";
            return Directory.Exists(databasePath);
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            try
            {
                var databasePath = $@"{DataPath}\{databaseName}";

                if (Directory.Exists(databasePath))
                {
                    Console.WriteLine($"La base de datos '{databaseName}' ya existe.");
                    return OperationStatus.Warning;
                }

                Directory.CreateDirectory(databasePath);
                Console.WriteLine($"Base de datos '{databaseName}' creada exitosamente.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la base de datos '{databaseName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }

        public OperationStatus CreateTable(string databaseName, string tableName, (string ColumnName, string DataType)[] columnDefinitions)
        {
            try
            {
                var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

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
                return OperationStatus.Success;  // Retorna éxito si la tabla se crea correctamente
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la tabla '{tableName}' en la base de datos '{databaseName}': {ex.Message}");
                return OperationStatus.Error;  // Retorna error en caso de excepción
            }
        }


        public OperationStatus InsertRow(string databaseName, string tableName, string[] rowValues)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            try
            {
                using (FileStream stream = new FileStream(tablePath, FileMode.Append))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Escribir los valores de la fila
                    foreach (var value in rowValues)
                    {
                        writer.Write(value);
                    }
                }

                Console.WriteLine($"Fila insertada exitosamente en la tabla '{tableName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al insertar en la tabla '{tableName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }

        public OperationStatus Select(string databaseName, string tableName)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            try
            {
                using (FileStream stream = File.Open(tablePath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Leer la cantidad de columnas
                    int columnCount = reader.ReadInt32();
                    var columnDefinitions = new (string ColumnName, string DataType)[columnCount];

                    // Leer las definiciones de las columnas
                    for (int i = 0; i < columnCount; i++)
                    {
                        columnDefinitions[i] = (reader.ReadString(), reader.ReadString());
                    }

                    // Leer las filas
                    while (stream.Position < stream.Length)
                    {
                        for (int i = 0; i < columnCount; i++)
                        {
                            var value = reader.ReadString();
                            Console.Write($"{columnDefinitions[i].ColumnName}: {value} ");
                        }
                        Console.WriteLine();
                    }

                    return OperationStatus.Success;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer de la tabla '{tableName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }

        public List<Dictionary<string, string>> SelectColumns(string databaseName, string tableName, string[] selectedColumns)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return null;
            }

            try
            {
                var results = new List<Dictionary<string, string>>();

                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Leer la cantidad de columnas
                    int columnCount = reader.ReadInt32();
                    var columns = new List<(string ColumnName, string DataType)>();

                    // Leer las definiciones de las columnas
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columns.Add((columnName, dataType));
                    }

                    // Filtrar las columnas solicitadas por el usuario
                    var columnFilter = columns.Where(c => selectedColumns.Contains(c.ColumnName)).ToList();

                    if (!columnFilter.Any())
                    {
                        Console.WriteLine("Las columnas solicitadas no existen en la tabla.");
                        return null;
                    }

                    // Leer las filas y seleccionar las columnas solicitadas
                    while (stream.Position < stream.Length)
                    {
                        var row = new Dictionary<string, string>();

                        foreach (var column in columnFilter)
                        {
                            string value = reader.ReadString();
                            row[column.ColumnName] = value;
                        }

                        results.Add(row);
                    }
                }

                return results;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la tabla '{tableName}': {ex.Message}");
                return null;
            }
        }

        public List<Dictionary<string, string>> SelectAll(string databaseName, string tableName)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return null;
            }

            try
            {
                var results = new List<Dictionary<string, string>>();

                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Leer la cantidad de columnas
                    int columnCount = reader.ReadInt32();
                    var columns = new List<(string ColumnName, string DataType)>();

                    // Leer las definiciones de las columnas
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columns.Add((columnName, dataType));
                    }

                    // Leer las filas
                    while (stream.Position < stream.Length)
                    {
                        var row = new Dictionary<string, string>();

                        foreach (var column in columns)
                        {
                            string value = reader.ReadString();
                            row[column.ColumnName] = value;
                        }

                        results.Add(row);
                    }
                }

                return results;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la tabla '{tableName}': {ex.Message}");
                return null;
            }
        }





    }
}
