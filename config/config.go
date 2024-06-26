// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	// KeyConnection - string for connect to db2.
	KeyConnection string = "connection"
	// KeyTable database table name.
	KeyTable string = "table"
	// KeyColumns is a config name for columns.
	KeyColumns = "columns"
	// KeyPrimaryKeys is the list of the column names.
	KeyPrimaryKeys string = "primaryKeys"
	// KeyBatchSize is a config name for a batch size.
	KeyBatchSize = "batchSize"
	// KeyOrderingColumn is a config name for an ordering column.
	KeyOrderingColumn = "orderingColumn"
	// KeySnapshot is a config name for snapshotMode.
	KeySnapshot = "snapshot"
	// snapshotDefault is a default value for the Snapshot field.
	snapshotDefault = true

	// defaultBatchSize is a default value for a BatchSize field.
	defaultBatchSize = 1000
)

// Config represents configuration needed for Snowflake.
type Config struct {
	// Connection string connection to snowflake DB.
	// Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
	Connection string `validate:"required"`
	// Table name.
	Table string `validate:"required,max=255"`
	// List of columns from table, by default read all columns.
	Columns []string `validate:"contains_or_default=Keys OrderingColumn,dive,max=128"`
	// Keys is the list of the column names that records should use for their `Key` fields.
	Keys []string `validate:"dive,max=251"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `key:"orderingColumn" validate:"required,max=251"`
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool
	// BatchSize - size of batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// Parse attempts to parse plugins.Config into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Connection:     cfg[KeyConnection],
		Table:          cfg[KeyTable],
		OrderingColumn: cfg[KeyOrderingColumn],
		BatchSize:      defaultBatchSize,
		Snapshot:       snapshotDefault,
	}

	// Columns in snowflake is uppercase.
	if colsRaw := cfg[KeyColumns]; colsRaw != "" {
		config.Columns = strings.Split(strings.ToUpper(colsRaw), ",")
	}

	if keysRaw := cfg[KeyPrimaryKeys]; keysRaw != "" {
		config.Keys = strings.Split(strings.ToUpper(keysRaw), ",")
	}

	if cfg[KeyOrderingColumn] != "" {
		config.OrderingColumn = strings.ToUpper(config.OrderingColumn)
	}

	if cfg[KeyBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfg[KeyBatchSize])
		if err != nil {
			return Config{}, errors.New(`"batchSize" config value must be int`)
		}

		config.BatchSize = batchSize
	}

	if cfg[KeySnapshot] != "" {
		snapshot, err := strconv.ParseBool(cfg[KeySnapshot])
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", KeySnapshot, err)
		}

		config.Snapshot = snapshot
	}

	if err := Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}


// DestConfig represents configuration needed for Snowflake as a destination.
type DestConfig struct {
	// Connection string connection to snowflake DB.
	// Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
	Connection string `validate:"required"`
	// Table name.
	BatchSize int `validate:"gte=1,lte=100000"`
}

// Parse attempts to parse plugins.Config into a Config struct.
func ParseDest(cfg map[string]string) (DestConfig, error) {
	config := DestConfig{
		Connection:     cfg[KeyConnection],
		BatchSize:      defaultBatchSize,
	}
	if cfg[KeyBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfg[KeyBatchSize])
		if err != nil {
			return DestConfig{}, errors.New(`"batchSize" config value must be int`)
		}

		config.BatchSize = batchSize
	}
	if err := Validate(&config); err != nil {
		return DestConfig{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}
