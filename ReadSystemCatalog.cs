using System;
using System.IO;

class ReadSystemCatalog
{
    static void Main(string[] args)
    {
        string catalogPath = @"C:\TinySql\Data\SystemCatalog";  // Ruta del System Catalog

        // Leer los índices
        ReadIndexCatalog(Path.Combine(catalogPath, "indices.txt"));

        // Leer las columnas de SystemColumns.table
        ReadTableCatalog(Path.Combine(catalogPath, "SystemColumns.table"));

        // Leer las columnas de SystemIndexes.table
        ReadTableCatalog(Path.Combine(catalogPath, "SystemIndexes.table"));
    }

    static void ReadIndexCatalog(string filePath)
    {
        if (File.Exists(filePath))
        {
            Console.WriteLine("Indices from SystemCatalog:");
            string[] lines = File.ReadAllLines(filePath);
            foreach (string line in lines)
            {
                Console.WriteLine(line);
            }
        }
        else
        {
            Console.WriteLine("The indices file does not exist.");
        }
    }

    static void ReadTableCatalog(string filePath)
    {
        if (File.Exists(filePath))
        {
            Console.WriteLine($"\nReading catalog data from {Path.GetFileName(filePath)}:");
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    try
                    {
                        // Leer cada entrada en el archivo binario
                        string databaseName = reader.ReadString();
                        string tableName = reader.ReadString();
                        string columnName = reader.ReadString();
                        string dataType = reader.ReadString();

                        Console.WriteLine($"Database: {databaseName}, Table: {tableName}, Column: {columnName}, DataType: {dataType}");
                    }
                    catch (EndOfStreamException)
                    {
                        Console.WriteLine("Reached the end of the file unexpectedly.");
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error reading the file: {ex.Message}");
                        break;
                    }
                }
            }
        }
        else
        {
            Console.WriteLine($"The file {Path.GetFileName(filePath)} does not exist.");
        }
    }
}
