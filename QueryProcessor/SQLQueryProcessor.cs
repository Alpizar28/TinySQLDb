using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;

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
