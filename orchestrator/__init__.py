import azure.durable_functions as df

def main(context: df.DurableOrchestrationContext):
    output = []
    output.append(context.call_activity('mapper', "data"))
    output.append(context.call_activity('shuffler', "data"))
    output.append(context.call_activity('reducer', "data"))
    return output
