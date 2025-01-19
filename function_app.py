import azure.functions as func
import azure.durable_functions as df
from app import myApp  # Import from app.py
import orchestrator  # Import orchestrator AFTER defining myApp

# HTTP Trigger to start the orchestrator
@myApp.route(route="orchestrators/{functionName}")
@myApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response
