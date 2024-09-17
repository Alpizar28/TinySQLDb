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

        // Crear tabla en una base de datos específica
        public OperationStatus CreateTable(string databaseName, string tableName, string[] columnDefinitions)
        {
            try
            {
                // Definir la ruta de la tabla
                var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

                // Verificar si la tabla ya existe
                if (File.Exists(tablePath))
                {
                    Console.WriteLine($"La tabla '{tableName}' ya existe en la base de datos '{databaseName}'.");
                    return OperationStatus.Warning; // Retorna advertencia si la tabla ya existe
                }

                // Crear la tabla y escribir las definiciones de las columnas
                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Escribir las definiciones de las columnas
                    foreach (var column in columnDefinitions)
                    {
                        writer.Write(column);
                    }
                }

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en la base de datos '{databaseName}'.");
                return OperationStatus.Success;  // Retorna éxito si se creó correctamente
            }
            catch (Exception ex)
            {
                // Manejar cualquier error durante la creación
                Console.WriteLine($"Error al crear la tabla '{tableName}' en la base de datos '{databaseName}': {ex.Message}");
                return OperationStatus.Error;  // Retorna error en caso de excepción
            }
        }



        // Seleccionar datos desde una tabla
        public OperationStatus Select(string databaseName, string tableName)
        {
            var tablePath = $@"{DataPath}\{databaseName}\{tableName}.Table";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return OperationStatus.Error;
            }

            using (FileStream stream = File.Open(tablePath, FileMode.Open))
            using (BinaryReader reader = new(stream))
            {
                Console.WriteLine(reader.ReadInt32());
                Console.WriteLine(reader.ReadString());
                Console.WriteLine(reader.ReadString());
                return OperationStatus.Success;
            }
        }
    }
}
