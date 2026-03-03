
```
diagrid subscription create orders-subscription   --component pubsub   --topic orders   --route /start-workflow   --scopes workflow-app
export CATALYST_HTTP_ENDPOINT=https://http-prj588196.cloud.r1.diagrid.io:443 
export APP_ID_API_TOKEN=

curl -X POST "$CATALYST_HTTP_ENDPOINT/v1.0/publish/pubsub/orders" \
  -H "Content-Type: application/json" \
  -H "dapr-api-token: $APP_ID_API_TOKEN" \
  -d '{"orderId":123}'

diagrid dev run --app-id order-api --app-port 8000  -- uvicorn order-api.main:app --host 0.0.0.0 --port 8000
diagrid dev run --app-id worflow --app-port 8001  -- uvicorn workflow-worker.main:app --host 0.0.0.0 --port 8001
diagrid dev run --app-id inventory-svc --app-port 8002 -- uvicorn inventory-svc.main:app --host 0.0.0.0 --port 8002
diagrid dev run --app-id payment-svc --app-port 8003   -- uvicorn payment-svc.main:app --host 0.0.0.0 --port 8003
```


