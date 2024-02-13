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
	"errors"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/mock"
)

func TestDestination_Configure(t *testing.T) {
	s := Destination{}

	tests := []struct {
		name        string
		cfg         map[string]string
		wantErr     bool
		expectedErr error
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				config.KeyConnection:     "user:password@my_organization-my_account/mydb",
				config.KeyTable:          "customer",
				config.KeyPrimaryKeys:    "id",
				config.KeyOrderingColumn: "id",
			},
			expectedErr: nil,
		},
		{
			name: "missing connection",
			cfg: map[string]string{
				config.KeyTable:          "customer",
				config.KeyColumns:        "",
				config.KeyPrimaryKeys:    "id",
				config.KeyOrderingColumn: "id",
			},
			expectedErr: errors.New("validate config: Connection value must be set"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Configure(context.Background(), tt.cfg)
			if err != nil && errors.Is(err, tt.expectedErr) {
				t.Errorf("got = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}

func TestDestination_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(sdk.StructuredData)
		st["key"] = "value"

		record := sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: nil,
			Key:      st,
			Payload:  sdk.Change{After: st},
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Destination{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("run query: failed"))

		s := Destination{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

		s := Destination{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestDestination_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(nil)

		s := Destination{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop().Return(errors.New("some error"))

		s := Destination{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}