package types

type Config struct {
	Datasource Datasource `yaml:"datasource"`
}

type Datasource struct {
	Host     string `yaml:"host"`
	Port     int32  `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}
