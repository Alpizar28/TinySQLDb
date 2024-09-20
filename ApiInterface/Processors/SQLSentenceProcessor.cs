using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor : IProcessor
    {
        public Request Request { get; }

        public SQLSentenceProcessor(Request request)
        {
            this.Request = request;
        }

        public Response Process()
        {
            var sentence = this.Request.RequestBody;
            var processor = new SQLQueryProcessor();
            var result = processor.Execute(sentence);
            var response = this.ConvertToResponse(result);
            return response;
        }

        private Response ConvertToResponse(OperationStatus result)
        {
            return new Response
            {
                Status = result,
                Request = this.Request,
                ResponseBody = string.Empty
            };
        }
    }
}
