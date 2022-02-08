package playfab

type noopLogger struct{}

func (*noopLogger) Debug(format string, v ...interface{}) {}
func (*noopLogger) Info(format string, v ...interface{})  {}
func (*noopLogger) Warn(format string, v ...interface{})  {}
func (*noopLogger) Error(format string, v ...interface{}) {}
