package app

import "github.com/local/polymarket-process-service/pkg/config"

type Config = config.Config

func LoadConfig() Config {
	return config.Load()
}
