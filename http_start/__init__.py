import azure.functions as func
import azure.durable_functions as df

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    function_name = req.params.get("functionName")

    if not function_name:
        return func.HttpResponse("Missing 'functionName' in query string", status_code=400)

    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)

    return response
