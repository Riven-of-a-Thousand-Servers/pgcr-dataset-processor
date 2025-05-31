package config

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func ReadConfig(yamlFile []byte) (*Config, error) {
	expandedYaml := os.ExpandEnv(string(yamlFile))

	var config Config
	err := yaml.Unmarshal([]byte(expandedYaml), &config)
	if err != nil {
		log.Panicf("Error marshaling datasource: %v", err)
		return nil, err
	}

	switch {
	case config.BatchSize <= 0:
		return nil, fmt.Errorf("Batch size cannot be zero or negative")
	case config.Datasource.Host == "":
		return nil, fmt.Errorf("Datasource host cannot be empty")
	case config.Datasource.Port == 0:
		return nil, fmt.Errorf("Datasource port cannot be empty")
	case config.Directory == "":
		return nil, fmt.Errorf("Root directory cannot be empty")
	case config.Workers <= 0:
		return nil, fmt.Errorf("Number of workers cannot be zero or negative")
	}

	return &config, nil
}
