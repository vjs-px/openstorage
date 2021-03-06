/*
Package sdk is the gRPC implementation of the SDK gRPC server
Copyright 2018 Portworx

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package sdk

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/libopenstorage/openstorage/api"
	policy "github.com/libopenstorage/openstorage/pkg/storagepolicy"
	"github.com/libopenstorage/openstorage/volume"
	volumedrivers "github.com/libopenstorage/openstorage/volume/drivers"
	mockdriver "github.com/libopenstorage/openstorage/volume/drivers/mock"
	"github.com/stretchr/testify/assert"
)

func TestNewSdkServerBadParameters(t *testing.T) {
	setupMockDriver(&testServer{}, t)
	s, err := New(nil)
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "configuration")

	s, err = New(&ServerConfig{})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Must provide unix domain")

	s, err = New(&ServerConfig{
		Net: "test",
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Must provide unix domain")

	sp, err := policy.Inst()
	assert.NoError(t, err)
	assert.NotNil(t, sp)

	s, err = New(&ServerConfig{
		Net:           "test",
		Socket:        "blah",
		RestPort:      "2344",
		AccessOutput:  ioutil.Discard,
		AuditOutput:   ioutil.Discard,
		StoragePolicy: sp,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Address must be")

	// Add driver to registry
	mc := gomock.NewController(t)
	defer mc.Finish()
	m := mockdriver.NewMockVolumeDriver(mc)
	volumedrivers.Add("mock", func(map[string]string) (volume.VolumeDriver, error) {
		return m, nil
	})
	defer volumedrivers.Remove("mock")
	s, err = New(&ServerConfig{
		Net:          "test",
		Address:      "blah",
		DriverName:   "mock",
		RestPort:     "2345",
		AccessOutput: ioutil.Discard,
		AuditOutput:  ioutil.Discard,
	})
	assert.Nil(t, s)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Must provide unix domain")
}

func TestSdkClusterInspectCurrent(t *testing.T) {

	// Create server and client connection
	s := newTestServer(t)
	defer s.Stop()

	// Create response
	uuid := "id"
	name := "name"
	cluster := api.Cluster{
		Id:     name,
		NodeId: "somenodeid",
		Status: api.Status_STATUS_NOT_IN_QUORUM,
	}
	s.MockCluster().EXPECT().Enumerate().Return(cluster, nil).Times(1)
	s.MockCluster().EXPECT().Uuid().Return(uuid).Times(1)

	// Setup client
	c := api.NewOpenStorageClusterClient(s.Conn())

	// Get info
	r, err := c.InspectCurrent(context.Background(), &api.SdkClusterInspectCurrentRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, r.GetCluster())
	assert.Equal(t, cluster.Id, r.GetCluster().GetName())
	assert.Equal(t, cluster.Status, r.GetCluster().GetStatus())
	assert.Equal(t, uuid, r.GetCluster().GetId())
}
