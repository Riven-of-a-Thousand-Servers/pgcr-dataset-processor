package config

type Config struct {
	Workers    int64      `yaml:"workers"`
	Directory  string     `yaml:"directory"`
	Datasource Datasource `yaml:"datasource"`
	BatchSize  int64      `yaml:"batchSize"`
}

type Datasource struct {
	Host     string `yaml:"host"`
	Port     int32  `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}
