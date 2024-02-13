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

package destination

import (
	"bytes"
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
)

// Destination connector.
type Destination struct {
	// required
	sdk.UnimplementedDestination
	// config details
	config    config.DestConfig
	// snowflake repository
	snowflake repository.Repository
	// known table mappings
	tblMap    map[string]TableInfo
}

// stored struct for a single table
type TableInfo struct {
	// list of table column names
	colname []string
	// list of table column types
	coltype []string
	// row format string
	fmtString string
}

// New initialises a new destination.
func New() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyConnection: {
			Default:     "",
			Required:    true,
			Description: "Snowflake connection string.",
		},
		config.KeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of batch",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		return err
	}

	d.config = cfg

	return nil
}

// Open prepare the plugin to start writing records from the given position.
func (d *Destination) Open(ctx context.Context) error {
	// Create storage.
	s, err := d.snowflake.Create(d.config.Connection)
	if err != nil {
		return fmt.Errorf("error on repo creation: %w", err)
	}
	d.snowflake = s
	return nil
}

// Write a batch of records to snowflake
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// lazy caching of tables we know about
	var tablesNeedingInit []string

	// we need to partition these records by table, then for each table we can
	// do a merge statement to handle all records simultaneously

	tabMap := make(map[string][]*sdk.Record)

	// partition each record into per-table batches
	for _, r := range records {
		tab := r.Metadata["table"]
		tabMap[tab] = append(tabMap[tab], &r)

		if _, found := d.tableinfo[tab]; !found {
			tablesNeedingInit = append(tablesNeedingInit, tab)
		}
	}

	// check for any currently untracked tables
	if len(tablesNeedingInit) > 0 {
		for _, t := range tablesNeedingInit {
			d.initTable(tablesNeedingInit)
		}
	}

	// now iterate over our groups to handle a merge for each
	for tab, g := range (tabMap) {
		d.mergeTable(ctx, t, g)
	}

	return len(records), nil
}

// Teardown gracefully shutdown connector.
func (d *Destination) Teardown(ctx context.Context) error {
	return nil
}

func (d *Destination) mergeTable(ctx context.Context, tableName string, records []*sdk.Record) {
	if ti, found := d.tableinfo[tableName]; !found {
		panic("trying to merge unknown table %v", tableName)
	}

	// let's ensure there is a staging table that exists and is loaded and we can merge in the contents
	setup_sql := fmt.Sprintf(`
CREATE TEMPORARY TABLE IF NOT EXISTS "%s" AS
SELECT * FROM "%s", char(' ') AS operation LIMIT 0
`, stageName, tableName)		// TODO: quote
	d.conn.ExecContext(ctx, setup_sql)

	// todo: store the column type here from the original source?

	// truncate said table
	d.conn.ExecContext(ctx, `truncate table "%s"`, stageName)

	// see about the columns

	// load data into the table
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s" VALUES `, stageName)

	for i, r := range records {
		fmt.Fprintf(&b, `(%s)`, outrec(r))
		if i < len(r) {
			fmt.Fprint(&b, ',')
		}
	}

	d.conn.ExecContext(ctx, )
	copy into table from records list -- with operation
	// do the merge itself
	merge key columns
}

// populate our table cache/set for tables we haven't seen before
func (d *Destination) initTable(ctx context.Context, tableName string) {
	// let's ensure we don't init the table a second time
	if _, found := d.tableinfo[tableName]; found {
		panic("trying to reinit an already inited table")
	}
	// we pull information from the dest tables that we can
	info := &TableInfo{}
	info.stageTableName = fmt.Sprintf('%s_stage', tableName)

	rows, err := d.snowflake.conn.QueryContext(ctx,
		`SELECT column_name, column_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?`,
		tableName)

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		// handle error
		return
	}

	var builder = strings.Builder
	var colname, coltype string

	fld := 0

	for rows.Next() {
		rows.Scan(&colname, &coltype)

		if fld > 0 {
			builder.WriteRune(',')
		}
		builder.WriteString("'%s'")

		info.colnames = append(info.colnames, colname)
		info.coltypes = append(info.coltypes, coltype)
		fld += 1
	}
	d.tableinfo[tableName] = info
}
