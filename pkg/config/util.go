package config

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

func ReadConfig() Config {
	yamlFile, err := os.ReadFile("config.yml")
	if err != nil {
		log.Panicf("Unable to read config file: %v", err)
	}

	expandedYaml := os.ExpandEnv(string(yamlFile))

	var config Config
	err = yaml.Unmarshal([]byte(expandedYaml), &config)
	if err != nil {
		log.Panicf("Error marshaling datasource: %v", err)
	}
	return config
}
