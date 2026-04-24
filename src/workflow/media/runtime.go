package mediaworkflow

type workflowRuntime interface {
	GetRunConfig() map[string]any
	ReadKV(key string) string
	WriteKV(key, value string)
	SetRequestContextValue(requestID any, key string, value any) bool
	GetRequestContextValue(requestID any, key string) (any, bool)
	DeleteRequestContextValue(requestID any, key string)
	ClearRequestContext(requestID any)
	GetNestedResponses(requestID any) ([]map[string]any, bool)
	DispatchNestedRequestDirect(sourceRequest map[string]any, targets []string, outgoing map[string]any)
	DispatchNestedRequestEO(sourceRequest map[string]any, targets []string, outgoing map[string]any)
}
