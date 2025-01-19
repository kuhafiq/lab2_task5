import azure.durable_functions as df

@df.orchestration_trigger
def master_orchestrator(context: df.DurableOrchestrationContext):
    tasks = []
    tasks.append(context.call_activity("mapper", "data"))
    tasks.append(context.call_activity("shuffler", "data"))
    tasks.append(context.call_activity("reducer", "data"))

    return context.task_all(tasks)
