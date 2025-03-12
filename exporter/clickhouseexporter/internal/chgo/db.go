package chgo

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
	"time"
)

type ChConfig struct {
	Address          string
	User             string
	Password         string
	Database         string
	Table            string
	Compression      string
	CompressionLevel int
	TLS              bool
	DialTimeout      time.Duration
	ClientName       string
	Settings         map[string]string

	BatchMetricsEnabled    bool
	BatchMetricsTableName  string
	BatchMetricsConfigName string
}

func connectDB(ctx context.Context, db **ch.Client, cfg *ChConfig) error {
	compressMethod, _ := compress.MethodString(cfg.Compression)

	var tlsCfg *tls.Config
	if cfg.TLS {
		tlsCfg = &tls.Config{}
	}

	opts := ch.Options{
		Address:          cfg.Address,
		User:             cfg.User,
		Password:         cfg.Password,
		Database:         cfg.Database,
		Compression:      ch.Compression(compressMethod),
		CompressionLevel: ch.CompressionLevel(cfg.CompressionLevel),
		DialTimeout:      cfg.DialTimeout,
		ClientName:       cfg.ClientName,
		TLS:              tlsCfg,
		Settings:         make([]ch.Setting, 1, 1+len(cfg.Settings)),
	}

	opts.Settings[0] = ch.Setting{Key: "allow_json_type", Value: "1"}
	for name, value := range cfg.Settings {
		opts.Settings = append(opts.Settings, ch.Setting{Key: name, Value: value})
	}

	c, err := ch.Dial(ctx, opts)
	if err != nil {
		return fmt.Errorf("chgo dial: %w", err)
	}
	*db = c

	return nil
}

func closeDB(db **ch.Client) error {
	if *db == nil {
		return nil
	}

	err := (*db).Close()
	*db = nil
	if err != nil {
		return fmt.Errorf("chgo close: %w", err)
	}

	return nil
}
