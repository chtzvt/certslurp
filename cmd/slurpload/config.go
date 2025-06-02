package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	MaxConns     int    `mapstructure:"max_conns"`
	BatchSize    int    `mapstructure:"batch_size"`
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password,omitempty"`
	DatabaseName string `mapstructure:"database"`
	SSLMode      string `mapstructure:"ssl_mode"`
}

type ServerConfig struct {
	ListenAddr string `mapstructure:"listen_addr"`
}

type ProcessingConfig struct {
	InboxDir          string        `mapstructure:"inbox_dir"`
	InboxPatterns     string        `mapstructure:"inbox_patterns"`
	InboxPollInterval time.Duration `mapstructure:"inbox_poll"`
	EnableWatcher     bool          `mapstructure:"enable_watcher"`
	DoneDir           string        `mapstructure:"done_dir"`
	FlushInterval     time.Duration `mapstructure:"flush_interval"`
	FlushThreshold    int64         `mapstructure:"flush_thresh"`
	FlushLimit        int64         `mapstructure:"flush_limit"`
}

type MetricsConfig struct {
	LogStatEvery int64 `mapstructure:"log_stat_every"`
}

type SlurploadConfig struct {
	Database   DatabaseConfig   `mapstructure:"database"`
	Server     ServerConfig     `mapstructure:"server"`
	Processing ProcessingConfig `mapstructure:"processing"`
	Metrics    MetricsConfig    `mapstructure:"metrics"`
}

func loadConfig(cfgFile string) (*SlurploadConfig, error) {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("slurpload")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/etc/slurpload/")
	}

	viper.SetEnvPrefix("SLURPLOAD") // env vars like SLURPLOAD_PROCESSING__INBOX_DIR
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "__"))

	viper.SetDefault("database.max_conns", 8)
	viper.SetDefault("database.batch_size", 100)
	viper.SetDefault("metrics.log_stat_every", 1000)
	viper.SetDefault("processing.inbox_poll", 2*time.Second)
	viper.SetDefault("processing.flush_interval", 10*time.Second)
	viper.SetDefault("processing.flush_thresh", 100_000)
	viper.SetDefault("processing.flush_limit", 10_000_000)

	viper.BindEnv("database.max_conns")
	viper.BindEnv("database.batch_size")
	viper.BindEnv("database.host")
	viper.BindEnv("database.port")
	viper.BindEnv("database.username")
	viper.BindEnv("database.password")
	viper.BindEnv("database.database")
	viper.BindEnv("database.ssl_mode")

	viper.BindEnv("server.listen_addr")

	viper.BindEnv("processing.inbox_dir")
	viper.BindEnv("processing.inbox_patterns")
	viper.BindEnv("processing.inbox_poll")
	viper.BindEnv("processing.enable_watcher")
	viper.BindEnv("processing.done_dir")

	viper.BindEnv("metrics.log_stat_every")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	if viper.ConfigFileUsed() != "" {
		log.Printf("Loaded config from %s", viper.ConfigFileUsed())
	}

	var cfg SlurploadConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if cfg.Database.Host == "" || cfg.Database.DatabaseName == "" {
		return nil, errors.New("database.host and database.database must be set (check config/env/flags)")
	}

	return &cfg, nil
}

func openDatabase(cfg *SlurploadConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", buildDSN(cfg))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.Database.MaxConns)
	db.SetMaxIdleConns(cfg.Database.MaxConns)
	return db, nil
}

func buildDSN(cfg *SlurploadConfig) string {
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		cfg.Database.Host, cfg.Database.Port, cfg.Database.Username, cfg.Database.DatabaseName, cfg.Database.SSLMode)
	if cfg.Database.Password != "" {
		dsn += " password=" + cfg.Database.Password
	}
	return dsn
}
