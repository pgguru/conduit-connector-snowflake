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
	"context"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
)

// Destination connector.
type Destination struct {
	// required
	sdk.UnimplementedDestination
	// config details
	config    config.DestConfig
	// snowflake repository
	snowflake *repository.Snowflake
	// known table mappings
	knownTables    map[string]*TableInfo
}

// stored struct for a single table
type TableInfo struct {
	// list of table column names
	colnames []string
	// list of table column types
	coltypes []string
	// row format string
	fmtString string
	// customized merge query
	mergeQuery string
	// stage table name
	stageName string
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
	cfg, err := config.ParseDest(cfgRaw)
	if err != nil {
		return err
	}

	d.config = cfg

	return nil
}

// Open prepare the plugin to start writing records from the given position.
func (d *Destination) Open(ctx context.Context) error {
	// Create storage.
	s, err := repository.Create(ctx,d.config.Connection)
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

		if _, found := d.knownTables[tab]; !found {
			tablesNeedingInit = append(tablesNeedingInit, tab)
		}
	}

	// check for any currently untracked tables
	if len(tablesNeedingInit) > 0 {
		for _, t := range tablesNeedingInit {
			d.initTable(ctx,t)
		}
	}

	// now iterate over our groups to handle a merge for each
	for t, g := range (tabMap) {
		d.mergeTable(ctx, t, g)
	}

	return len(records), nil
}

// Teardown gracefully shutdown connector.
func (d *Destination) Teardown(ctx context.Context) error {
	return nil
}

func (d *Destination) mergeTable(ctx context.Context, tableName string, records []*sdk.Record) {
	ti, found := d.knownTables[tableName]

	if !found {
		panic(fmt.Sprintf("trying to merge unknown table %v", tableName))
	}

	// truncate staging table
	d.snowflake.ExecContext(ctx, `truncate table "%s"`, ti.stageName)

	// load data into the table
	var b strings.Builder
	fmt.Fprintf(&b, `INSERT INTO "%s" VALUES `, ti.stageName)

	for i, r := range records {
		if i != 0 {
			b.WriteRune(',')
		}
		b.WriteRune('(')
		fmt.Fprintf(&b, ti.fmtString, r)
		b.WriteRune(')')
	}

	// our query is now ready, let's run the query to insert into stage table
	d.snowflake.ExecContext(ctx, b.String())

	// now in the stage table, let's process the batch with the merge query
	d.snowflake.ExecContext(ctx, ti.mergeQuery)
}

// populate our table cache/set for tables we haven't seen before
func (d *Destination) initTable(ctx context.Context, tableName string) {
	// let's ensure we don't init the table a second time
	if _, found := d.knownTables[tableName]; found {
		panic("trying to reinit an already inited table")
	}
	operationCol := "cdc_operation"
	// we pull information from the dest tables that we can
	info := &TableInfo{}
	info.stageName = fmt.Sprintf("%s_stage", tableName)

	rows, err := d.snowflake.QueryContext(ctx,
		`SELECT column_name, column_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?`,
		tableName)

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		// handle error
		return
	}

	var builder, updateBuilder, insertBuilder, keyMatchBuilder strings.Builder
	var colname, coltype string

	fld := 0

	for rows.Next() {
		rows.Scan(&colname, &coltype)

		if fld > 0 {
			builder.WriteRune(',')
			updateBuilder.WriteRune(',')
			insertBuilder.WriteRune(',')
		}
		builder.WriteString("'%s'")

		quote_col(updateBuilder, colname)
		updateBuilder.WriteRune('=')
		quote_tablecol(updateBuilder, info.stageName, colname)

		quote_tablecol(insertBuilder, info.stageName, colname)

		if strings.ContainsAny(colname, "\"\\") {
			panic("cannot support column names which need special quoting") // TODO: fix
		}
		info.colnames = append(info.colnames, colname)
		info.coltypes = append(info.coltypes, coltype)
		fld += 1
	}

	// keymatch builder uses the key columns--available from the metadata, so on first run we will have them
	keyCols, err := d.snowflake.GetPrimaryKeys(ctx, tableName)

	// we need PKs to do anything
	if err != nil {
		panic("couldn't determine primary key!")
	}

	for i, k := range keyCols {
		if i > 0 {
			keyMatchBuilder.WriteString(" AND ")
		}
		quote_tablecol(keyMatchBuilder, tableName, k)
		updateBuilder.WriteRune('=')
		quote_tablecol(keyMatchBuilder, info.stageName, k)
	}

	// we are not bothering with a builder here since this is one-time setup cost
	info.mergeQuery = fmt.Sprintf(`
MERGE INTO "%s" USING "%s" ON %s
WHEN MATCHED AND "%s" = 'D' THEN DELETE
WHEN MATCHED THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT ("%s") VALUES (%s)`,
		tableName,
		info.stageName,
		keyMatchBuilder.String(),
		operationCol,
		updateBuilder.String(),
		strings.Join(info.colnames, "\",\""),	// target columns
		insertBuilder.String())

	d.knownTables[tableName] = info

// 	// let's ensure there is a staging table that exists and is loaded and we can merge in the contents
// 	setup_sql := fmt.Sprintf(`
// CREATE TEMPORARY TABLE IF NOT EXISTS "%s" AS
// SELECT * FROM "%s", char(' ') AS operation LIMIT 0
// `, ti.stageName, tableName)		// TODO: quote
// 	d.conn.ExecContext(ctx, setup_sql)

}

// utility to double-quote a single-col identifier
func quote_col(b strings.Builder, s string) {
	b.WriteRune('"')
	b.WriteString(s)
	b.WriteRune('"')
}

// utility to double-quote and join a table/column
func quote_tablecol(b strings.Builder, s1, s2 string) {
	b.WriteRune('"')
	b.WriteString(s1)
	b.WriteString("\".\"")
	b.WriteString(s2)
	b.WriteRune('"')
}
