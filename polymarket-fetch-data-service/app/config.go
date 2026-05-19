package app

import "github.com/local/polymarket-fetch-data-service/pkg/config"

type Config = config.Config

func LoadConfig() Config {
	return config.Load()
}
