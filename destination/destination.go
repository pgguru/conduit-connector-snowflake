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
	"strconv"
	"strings"
	"time"

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
	// size of per-table batches
	batchSize int
	// length of per-table sync interval
	interval time.Duration
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
	// how many records since last sync
	count int
	// sync channel
	sync chan struct {}
	// our builder for adding records to the stage table
	insBuilder *strings.Builder
	// sequence number; monotonic and used for ties for changes on the same row in the same sync period
	seq int64
}

// New initialises a new destination.
func New() sdk.Destination {
	d := &Destination{
		knownTables: make(map[string]*TableInfo),
		batchSize: 5000,		// how many rows (per table) to insert into staging table
		interval: time.Minute,	// how frequently (per table) to merge the staging table into the main one
	}
	return sdk.DestinationWithMiddleware(d, sdk.DefaultDestinationMiddleware()...)
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
	sdk.Logger(ctx).Debug().Msg(fmt.Sprintf("Write() %d records", len(records)))
	// we need to partition these records by table, then for each table we can
	// do a merge statement to handle all records simultaneously

	// partition each record into per-table batches
	for _, r := range records {
		tab := r.Metadata["postgres.table"] // this is hard-coded assumption,
		// which isn't good for general case; if we support more than just
		// postgres as source then this should be revisited.

		var ti *TableInfo
		if myti, found := d.knownTables[tab]; !found {
			d.initTable(ctx, tab)
			ti = d.knownTables[tab]
		} else {
			ti = myti
		}

		// add our record to the local cache
		d.addRecord(ctx,ti,&r)
	}
	return len(records), nil
}

// Teardown gracefully shutdown connector.
func (d *Destination) Teardown(ctx context.Context) error {
	return nil
}

func (d *Destination) addRecord(ctx context.Context, ti *TableInfo, r *sdk.Record) {
	// load data into the table
	var b = ti.insBuilder

	if b == nil {
		ti.insBuilder = &strings.Builder{}
		b = ti.insBuilder
		b.Grow(200*d.batchSize)			// guess on an average row size; maybe make tunable?
		// TODO: what happens if we exceed this size? ideally we can grow as we want automatically, maybe double in size if we exceed
		fmt.Fprintf(b, `INSERT INTO %s ("%s",cdc_operation,cdc_serial) VALUES `,
			ti.stageName,
			strings.Join(ti.colnames, "\",\""),
		)
		ti.count = 0
	}

	var sd sdk.StructuredData

	if r.Operation == sdk.OperationDelete {
		sd = r.Key.(sdk.StructuredData)
	} else {
		sd = r.Payload.After.(sdk.StructuredData)
	}

	if ti.count > 0 {
		b.WriteRune(',')
	}
	b.WriteRune('(')
	// field by field values
	for i, f := range ti.colnames {
		if i > 0 {
			b.WriteRune(',')
		}
		quote_value(b, sd[f])
	}
	b.WriteRune(',')

	// now add our system columns

	// set the operation depending on whether we are a delete or not
	if r.Operation == sdk.OperationDelete {
		b.WriteRune('1')
	} else {
		b.WriteRune('0')
	}
	b.WriteRune(',')

	// increase and append our record seq number
	ti.seq += 1
	b.WriteString(strconv.FormatInt(ti.seq, 10))
	b.WriteRune(')')

	ti.count += 1

	if ti.count >= d.batchSize {
		ti.sync <- struct{}{}
	}
}

// populate our table cache/set for tables we haven't seen before -- this will
// fork off a new goroutine to handle things, so we might need to change a
// little of how we handle things if the number of tables is large.
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
		`SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?`,
		strings.ToUpper(tableName)) // TODO: add check for ToUpper()

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

		quote_col(&updateBuilder, colname)
		updateBuilder.WriteRune('=')
		quote_tablecol(&updateBuilder, info.stageName, colname)

		quote_tablecol(&insertBuilder, info.stageName, colname)

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
		quote_tablecol(&keyMatchBuilder, tableName, k)
		keyMatchBuilder.WriteRune('=')
		quote_tablecol(&keyMatchBuilder, info.stageName, k)
	}

	colList := strings.Join(info.colnames, "\",\"")

	latestChange := fmt.Sprintf(`
WITH RankedVersions AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY "%s" ORDER BY cdc_serial DESC) AS rn
  FROM
    %s
)
SELECT
    "%s",cdc_operation
FROM
  RankedVersions
WHERE
  rn = 1
`,
		strings.Join(keyCols,"\",\""),
		info.stageName,
		colList,
	)

	sdk.Logger(ctx).Info().Msg(latestChange)

	// we are not bothering with a builder here since this is one-time setup cost
	info.mergeQuery = fmt.Sprintf(`
MERGE INTO %s USING (%s) %s ON %s
WHEN MATCHED AND %s = 1 THEN DELETE
WHEN MATCHED THEN UPDATE SET %s
WHEN NOT MATCHED THEN INSERT ("%s") VALUES (%s)`,
		tableName,
		latestChange,
		info.stageName,
		keyMatchBuilder.String(),
		operationCol,
		updateBuilder.String(),
		colList,	// target columns
		insertBuilder.String())
	//		info.stageName)

	d.knownTables[tableName] = info

	// let's ensure there is a staging table that exists and is loaded and we can merge in the contents. TODO: do we need a duplicate index on this one too?
	setup_sql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s AS
SELECT *, 1 AS cdc_operation, 0::bigint as cdc_serial FROM %s LIMIT 0
`, info.stageName, tableName)		// TODO: quote

	_, err = d.snowflake.ExecContext(ctx, setup_sql)
	if err != nil {
		sdk.Logger(ctx).Error().Msg(err.Error())
		sdk.Logger(ctx).Info().Msg(setup_sql)
	}
	info.sync = make(chan struct{})

	// This sequence counter is monotonic and unique for rows added to the
	// staging table before the flush; each change is added to this counter, so
	// we'd need to be able to add records faster than a nanosecond in order for
	// there to be a possiblity of conflict in this table if this table worker
	// is canceled and restarted.  In all likelihood, this will not be
	// encountered, so as long as the upstream can store an int64 we should be
	// fine.

	info.seq = time.Now().UnixNano()

	//launch our worker
	info.startWorker(ctx, d)
}

// utility to double-quote a single-col identifier
func quote_col(b *strings.Builder, s string) {
	b.WriteRune('"')
	b.WriteString(s)
	b.WriteRune('"')
}

// utility to double-quote and join a table/column
func quote_tablecol(b *strings.Builder, s1, s2 string) {
	// b.WriteRune('"')
	b.WriteString(s1)
	// b.WriteString("\".\"")
	b.WriteString(".\"")
	b.WriteString(s2)
	b.WriteRune('"')
}

// utility to quote a value into a builder
func quote_value(b *strings.Builder, s any) {
	switch v := s.(type) {
	case string:
		b.WriteRune('\'')
		for _, c := range v {
			b.WriteRune(c)
			if c == '\'' {
				b.WriteRune(c)		// double single quotes
			}
		}
		b.WriteRune('\'')
	default:
		if v == nil {
			b.WriteString("NULL")
		} else {
			fmt.Fprintf(b, "%v", v)
		}
	}
}

// for a given worker, launch the table helper
func (t *TableInfo) startWorker(ctx context.Context, d *Destination) {
	timer := time.NewTimer(d.interval)
	go func() {
		for {
			select {
			case <-t.sync:
				sdk.Logger(ctx).Info().Msg("MANUAL SYNC")
				t.processBatch(ctx, d, true, false)
				timer.Reset(d.interval)
			case <-timer.C:
				sdk.Logger(ctx).Info().Msg("TIMER FIRED")
				t.processBatch(ctx, d, true, true)
				timer.Reset(d.interval)
			}
		}
	}()
}

// process our batch of records
func (ti *TableInfo) processBatch(ctx context.Context, d *Destination, flush, merge bool) {
	if flush && ti.count > 0 {
		// our query is now ready, let's run the query to insert into stage table
		_, err := d.snowflake.ExecContext(ctx, ti.insBuilder.String())
		if err != nil {
			sdk.Logger(ctx).Error().Msg(err.Error())
		}
		ti.insBuilder = nil
		ti.count = 0
	}

	if merge {
		// now in the stage table, let's process the batch with the merge query
		_, err := d.snowflake.ExecContext(ctx, ti.mergeQuery)
		if err != nil {
			sdk.Logger(ctx).Error().Msg(err.Error())
			sdk.Logger(ctx).Info().Msg(ti.mergeQuery)
		}
		// finally truncate staging table
		_, err = d.snowflake.ExecContext(ctx, fmt.Sprintf(`truncate table %s`, ti.stageName))
		if err != nil {
			sdk.Logger(ctx).Error().Msg(err.Error())
		}
	}
}
