using Entities;
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

        private const string DatabaseBasePath = @"C:\TinySql\Data\";

        public Store()
        {
            // Inicializar directorio base de datos
            Directory.CreateDirectory(DatabaseBasePath);
        }

        public bool DatabaseExists(string databaseName)
        {
            var databasePath = Path.Combine(DatabaseBasePath, databaseName);
            return Directory.Exists(databasePath);
        }

        public OperationStatus CreateDatabase(string databaseName)
        {
            try
            {
                var databasePath = Path.Combine(DatabaseBasePath, databaseName);

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
                var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

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
                        writer.Write(column.ColumnName);  // Nombre de la columna
                        writer.Write(column.DataType);    // Tipo de dato
                    }
                }

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en la base de datos '{databaseName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la tabla '{tableName}' en la base de datos '{databaseName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }

        public OperationStatus InsertRow(string databaseName, string tableName, string[] columns, string[] rowValues)
        {
            var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

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
                    // Escribir una marca para indicar el inicio de una nueva fila (opcional)
                    writer.Write((byte)1);  // Por ejemplo, 1 indica una nueva fila

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


        public List<Dictionary<string, string>> SelectAll(string databaseName, string tableName)
        {
            var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

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
                        // Leer la marca de inicio de fila (opcional)
                        byte rowMarker = reader.ReadByte();
                        if (rowMarker != 1)
                        {
                            Console.WriteLine("Formato de fila incorrecto.");
                            return null;
                        }

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

        public (string ColumnName, string DataType)[] GetTableDefinition(string databaseName, string tableName)
        {
            var tablePath = Path.Combine(DatabaseBasePath, databaseName, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return null;
            }

            try
            {
                using (FileStream stream = File.OpenRead(tablePath))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Leer la cantidad de columnas
                    int columnCount = reader.ReadInt32();
                    var columnDefinitions = new (string ColumnName, string DataType)[columnCount];

                    // Leer las definiciones de las columnas
                    for (int i = 0; i < columnCount; i++)
                    {
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();
                        columnDefinitions[i] = (columnName, dataType);
                    }

                    return columnDefinitions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la definición de la tabla '{tableName}': {ex.Message}");
                return null;
            }
        }
    }
}
