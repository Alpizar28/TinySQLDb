using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;
using System;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            var parts = sentence.Split(' ');

            if (sentence.StartsWith("CREATE DATABASE", StringComparison.OrdinalIgnoreCase))
            {
                if (parts.Length == 3)
                {
                    var databaseName = parts[2];
                    return new CreateDataBase().Execute(databaseName);
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para CREATE DATABASE.");
                    return OperationStatus.Error;
                }
            }

            if (sentence.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
            {
                var sentenceParts = sentence.Split(' ');
                if (sentenceParts.Length >= 5)
                {
                    var databaseName = sentenceParts[2].Split('.')[0];
                    var tableName = sentenceParts[2].Split('.')[1];

                    var columns = sentenceParts.Skip(3).ToArray();  // Extraer definiciones de las columnas
                    return new CreateTable().Execute(databaseName, tableName, columns);  // Pasar columnas a Execute
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para CREATE TABLE.");
                    return OperationStatus.Error;
                }
            }

            // Implementación de DROP TABLE <table-name>
            if (sentence.StartsWith("DROP TABLE", StringComparison.OrdinalIgnoreCase))
            {
                if (parts.Length == 3)
                {
                    var tableName = parts[2];
                    var store = Store.GetInstance();

                    // Definir el nombre de la base de datos (esto depende de cómo estás manejando las bases de datos)
                    var databaseName = "nombre_de_la_base_de_datos";  // Asegúrate de pasar el nombre correcto de la base de datos

                    // Verificar si la tabla está vacía
                    if (store.IsTableEmpty(databaseName, tableName))
                    {
                        return store.DropTable(databaseName, tableName);
                    }
                    else
                    {
                        Console.WriteLine("No se puede eliminar la tabla porque no está vacía.");
                        return OperationStatus.Error;
                    }
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para DROP TABLE.");
                    return OperationStatus.Error;
                }
            }


            // Verificar si la consulta es SELECT
            if (sentence.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
            {
                if (parts.Length >= 5)
                {
                    var databaseName = parts[3];
                    var tableName = parts[4];
                    return Store.GetInstance().Select(databaseName, tableName);
                }
                else
                {
                    Console.WriteLine("Sintaxis incorrecta para SELECT.");
                    return OperationStatus.Error;
                }
            }

            throw new UnknownSQLSentenceException();
        }
    }
}
