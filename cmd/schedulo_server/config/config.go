package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io"
	"os"
	"strconv"
)

var defaultConfig = Config{
	Dispatch: struct {
		WorkersNumber        int `yaml:"workersNumber,omitempty"`
		DefaultQueueCapacity int `yaml:"defaultQueueCapacity,omitempty"`
		MaxQueueCapacity     int `yaml:"maxQueueCapacity,omitempty"`
	}{
		WorkersNumber:        120,
		DefaultQueueCapacity: 1000,
		MaxQueueCapacity:     1500,
	},
	Database: struct {
		Url    string `yaml:"url,omitempty"`
		Driver string `yaml:"driver,omitempty"`
	}{
		Url:    envOrDefault("SCHEDULO_SQL_URL", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password='%s' sslmode=%s", "localhost", "5432", "job_scheduler", "job_scheduler", "job_scheduler", "disable")),
		Driver: envOrDefault("SCHEDULO_SQL_DRIVER", "postgres"),
	},
	Cache: struct {
		Addr string `yaml:"addr,omitempty"`
		Pass string `yaml:"pass,omitempty"`
		DB   int    `yaml:"db,omitempty"`
	}{
		Addr: envOrDefault("SCHEDULO_REDIS_ADDR", "localhost:6379"),
		Pass: envOrDefault("SCHEDULO_REDIS_PASS", ""),
		DB:   envOrDefaultInt("SCHEDULO_REDIS_DB", 0),
	},
	Input: struct {
		DefaultQueueCapacity int `yaml:"defaultQueueCapacity,omitempty"`
		MaxQueueCapacity     int `yaml:"maxQueueCapacity,omitempty"`
		MaxBulkLimit         int `yaml:"maxBulkLimit,omitempty"`
	}{
		DefaultQueueCapacity: 2500,
		MaxQueueCapacity:     3000,
		MaxBulkLimit:         1500,
	},
	System: struct {
		StacksNumber         int `yaml:"stacksNumber,omitempty"`
		DefaultStackCapacity int `yaml:"defaultStackCapacity,omitempty"`
		MaxStackCapacity     int `yaml:"maxStackCapacity,omitempty"`
	}{
		StacksNumber:         200,
		DefaultStackCapacity: 1000,
		MaxStackCapacity:     1500,
	},
	Network: struct {
		Port int    `yaml:"port,omitempty"`
		Addr string `yaml:"addr,omitempty"`
	}{
		Port: envOrDefaultInt("SCHEDULO_PORT", 9876),
		Addr: envOrDefault("SCHEDULO_ADDR", "localhost"),
	},
}

type Config struct {
	Dispatch struct {
		WorkersNumber        int `yaml:"workersNumber,omitempty"`
		DefaultQueueCapacity int `yaml:"defaultQueueCapacity,omitempty"`
		MaxQueueCapacity     int `yaml:"maxQueueCapacity,omitempty"`
	}

	Database struct {
		Url    string `yaml:"url,omitempty"`
		Driver string `yaml:"driver,omitempty"`
	}

	Cache struct {
		Addr string `yaml:"addr,omitempty"`
		Pass string `yaml:"pass,omitempty"`
		DB   int    `yaml:"db,omitempty"`
	}

	Input struct {
		DefaultQueueCapacity int `yaml:"defaultQueueCapacity,omitempty"`
		MaxQueueCapacity     int `yaml:"maxQueueCapacity,omitempty"`
		MaxBulkLimit         int `yaml:"maxBulkLimit,omitempty"`
	}

	System struct {
		StacksNumber         int `yaml:"stacksNumber,omitempty"`
		DefaultStackCapacity int `yaml:"defaultStackCapacity,omitempty"`
		MaxStackCapacity     int `yaml:"maxStackCapacity,omitempty"`
	}

	Network struct {
		Port int    `yaml:"port,omitempty"`
		Addr string `yaml:"addr,omitempty"`
	}
}

func GetConfig(path string) Config {
	f, err := os.Open(path)

	if err != nil {
		return DefaultConfig()
	}

	fstat, err := f.Stat()

	if err != nil {
		return DefaultConfig()
	}

	buf := make([]byte, fstat.Size())

	_, err = io.ReadFull(f, buf)

	if err != nil {
		return DefaultConfig()
	}

	var cfg = defaultConfig

	err = yaml.Unmarshal(buf, &cfg)

	if err != nil {
		return DefaultConfig()
	}

	return cfg
}

func DefaultConfig() Config {
	return defaultConfig
}

func envOrDefault(key string, def string) string {
	v, ok := os.LookupEnv(key)

	if !ok {
		return def
	}

	return v
}

func envOrDefaultInt(key string, def int) int {
	v, ok := os.LookupEnv(key)

	if !ok {
		return def
	}

	vInt, err := strconv.Atoi(v)

	if err != nil {
		return def
	}

	return vInt
}
