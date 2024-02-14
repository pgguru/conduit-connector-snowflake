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
)

func TestDestination_Configure(t *testing.T) {
	d := Destination{}

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
			},
			expectedErr: nil,
		},
		{
			name: "missing connection",
			cfg: map[string]string{
			},
			expectedErr: errors.New("validate config: Connection value must be set"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := d.Configure(context.Background(), tt.cfg)
			if err != nil && errors.Is(err, tt.expectedErr) {
				t.Errorf("got = %v, want %v", err, tt.expectedErr)
			}
		})
	}
}
