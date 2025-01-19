import azure.durable_functions as df
from app import myApp  # Import from app.py

@myApp.orchestration_trigger(context_name="context")
def master_orchestrator(context: df.DurableOrchestrationContext):
    # Step 1: Get Input Data from Blob Storage
    input_data = yield context.call_activity("get_input_data")
    
    # Step 2: Run Mappers in Parallel
    map_tasks = [context.call_activity("mapper", (line_num, line)) for line_num, line in input_data]
    map_results = yield context.task_all(map_tasks)
    
    # Step 3: Run Shuffler
    shuffled_data = yield context.call_activity("shuffler", map_results)
    
    # Step 4: Run Reducers in Parallel
    reduce_tasks = [context.call_activity("reducer", (word, counts)) for word, counts in shuffled_data.items()]
    reduce_results = yield context.task_all(reduce_tasks)
    
    return reduce_results
