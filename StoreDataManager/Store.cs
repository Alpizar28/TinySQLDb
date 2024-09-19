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

        // Crear base de datos con un nombre dinámico
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
                // Definir la ruta de la tabla
                var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

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
                        writer.Write(column.ColumnName);
                        writer.Write(column.DataType);
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

    }
}
