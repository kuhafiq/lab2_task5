import azure.functions as func
import azure.durable_functions as df

# Define Durable Function App BINMOHDN
myApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
