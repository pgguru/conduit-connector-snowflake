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

package iterator

import (
	"context"
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

// Iterator combined iterator.
type Iterator struct {
	snapshotIterator *snapshotIterator
	cdcIterator      *CDCIterator

	pos sdk.Position

	table   string
	columns []string
	key     string
}

// New iterator.
func New(
	ctx context.Context,
	conn, table,
	key, orderingColumn string,
	columns []string,
	batchSize int,
	snapshot bool,
	pos sdk.Position,
) (*Iterator, error) {
	it := &Iterator{
		table:   table,
		columns: columns,
		key:     key,
		pos:     pos,
	}

	snowflake, err := repository.Create(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("create snowflake repository: %w", err)
	}

	// First start.
	if pos == nil {
		err = prepareCDC(ctx, snowflake, table)
		if err != nil {
			return nil, fmt.Errorf("prepare cdc: %w", err)
		}
	}

	p, err := position.ParseSDKPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("parse sdk position: %w", err)
	}

	if snapshot && (p == nil || p.IteratorType == position.TypeSnapshot) {
		it.snapshotIterator, err = newSnapshotIterator(ctx, snowflake, table, orderingColumn, key, columns, batchSize, p)
		if err != nil {
			return nil, fmt.Errorf("setup snapshot iterator: %w", err)
		}
	} else {
		it.cdcIterator, err = setupCDCIterator(ctx, snowflake, table, key, columns, p, batchSize)
		if err != nil {
			return nil, fmt.Errorf("setup cdc iterator: %w", err)
		}
	}

	return it, nil
}

func prepareCDC(ctx context.Context, snowflake *repository.Snowflake, table string) error {
	// Check if table tracking table exist.
	isTableExist, err := snowflake.TableExists(ctx, getTrackingTable(table))
	if err != nil {
		return fmt.Errorf("check if table exist: %w", err)
	}

	if !isTableExist {
		// Prepare tracking table for consume stream.
		err = snowflake.CreateTrackingTable(ctx, getTrackingTable(table), table)
		if err != nil {
			return fmt.Errorf("create tracking table: %w", err)
		}
	}

	// Prepare stream for cdc iterator.
	err = snowflake.CreateStream(ctx, getStreamName(table), table)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	return nil
}

func setupCDCIterator(
	ctx context.Context,
	snowflake Repository,
	table, key string,
	columns []string,
	p *position.Position,
	batchSize int,
) (*CDCIterator, error) {
	var index, offset int
	if p != nil {
		offset = p.BatchID
	}

	if p != nil && p.IndexInBatch != 0 {
		index = p.IndexInBatch + 1
	}

	data, err := snowflake.GetTrackingData(ctx, getStreamName(table), getTrackingTable(table), columns,
		offset, batchSize)
	if err != nil {
		// Snowflake library sends request to abort query with query and to server when get context cancel.
		// But sometimes query was executed or didn't start execution.
		// On this case snowflake server return specific error:
		// 000605: Identified SQL statement is not currently executing.
		// Connector can't return this error and connector replace to
		// context cancel error
		// https://github.com/snowflakedb/gosnowflake/blob/master/restful.go#L449
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return nil, ctx.Err()
		}

		return nil, fmt.Errorf("get stream currentBatch: %w", err)
	}

	return NewCDCIterator(snowflake, table,
		columns, key, index, offset, batchSize, data), nil
}

// HasNext check ability to get next record.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	if i.snapshotIterator != nil {
		hasNext, err := i.snapshotIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot iterator has next: %w", err)
		}

		if hasNext {
			return true, nil
		}

		// Setup cdc iterator.
		cdcIterator, err := setupCDCIterator(ctx, i.snapshotIterator.snowflake,
			i.table, i.key, i.columns, nil, i.snapshotIterator.batchSize)
		if err != nil {
			return false, fmt.Errorf("setup cdc iterator: %w", err)
		}

		i.cdcIterator = cdcIterator
		i.snapshotIterator = nil

		hasNext, err = i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %w", err)
		}

		return hasNext, nil
	}

	if i.cdcIterator != nil {
		hasNext, err := i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %w", err)
		}

		return hasNext, nil
	}

	return false, ErrInvalidSetup
}

// Next get new record.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Next(ctx)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Next(ctx)
	}

	return sdk.Record{}, ErrInvalidSetup
}

// Ack check if record with position was recorded.
func (i *Iterator) Ack(ctx context.Context, rp sdk.Position) error {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Ack(ctx, rp)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Ack(ctx, rp)
	}

	return nil
}

// Stop iterator.
func (i *Iterator) Stop() error {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Stop()
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Stop()
	}

	return nil
}
