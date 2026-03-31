package shared

type Config struct {
	ServiceName          string
	HTTPHost             string
	HTTPPort             int
	LogDir               string
	LogRotateMaxMB       int
	LogRotateMaxBackups  int
	MetricsPath          string
	HealthPath           string
	InfoPath             string
	ReadTimeoutMS        int
	WriteTimeoutMS       int
	ShutdownTimeoutMS    int
	ProviderBaseURL      string
	ProviderHealthPath   string
	ProviderAPIKeyHeader string
	ProviderTimeoutMS    int
	ProviderRetryCount   int
	JWTSecret            string
	JWTIssuer            string
	JWTTTLMinutes        int
}

func DefaultConfig(service string, port int) Config {
	return Config{
		ServiceName:         service,
		HTTPHost:            "0.0.0.0",
		HTTPPort:            port,
		LogDir:              "runtime/logs/" + service,
		LogRotateMaxMB:      20,
		LogRotateMaxBackups: 5,
		MetricsPath:         "/metrics",
		HealthPath:          "/healthz",
		InfoPath:            "/info",
		ReadTimeoutMS:       5000,
		WriteTimeoutMS:      5000,
		ShutdownTimeoutMS:   5000,
	}
}
