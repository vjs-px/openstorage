/*
Copyright 2019 Portworx

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
package tester

import (
	"context"
	"testing"

	"github.com/libopenstorage/openstorage/api"
	volumeclient "github.com/libopenstorage/openstorage/api/client/volume"
	"github.com/stretchr/testify/assert"
)

func TestOsdTester(t *testing.T) {
	tester := New(t, &TesterConfig{})
	assert.Equal(t, "http://localhost:"+defaultRestPort, tester.UrlRest())
	assert.Equal(t, "http://localhost:"+defaultSdkRestPort, tester.UrlSdkRest())
	assert.Equal(t, "localhost:"+defaultSdkPort, tester.SdkEndpoint())

	for i := 0; i < 5; i++ {
		tester.Start()

		// Create a volume using the REST interface
		cl, err := volumeclient.NewAuthDriverClient(tester.UrlRest(), "fake", "v1", "", "", "fake")
		assert.NoError(t, err)
		name := "myvol"
		size := uint64(1234)
		req := &api.VolumeCreateRequest{
			Locator: &api.VolumeLocator{Name: name},
			Source:  &api.Source{},
			Spec: &api.VolumeSpec{
				HaLevel: 3,
				Size:    size,
				Format:  api.FSType_FS_TYPE_EXT4,
				Shared:  true,
			},
		}
		driverclient := volumeclient.VolumeDriver(cl)
		id, err := driverclient.Create(req.GetLocator(), req.GetSource(), req.GetSpec())
		assert.Nil(t, err)
		assert.NotEmpty(t, id)

		// Check using the SDK
		volumes := api.NewOpenStorageVolumeClient(tester.Conn())
		r, err := volumes.Inspect(context.Background(), &api.SdkVolumeInspectRequest{
			VolumeId: id,
		})
		assert.NoError(t, err)
		assert.NotNil(t, r)
		assert.Equal(t, "myvol", r.GetName())

		// Clear Kvdb
		tester.ClearKvdb()
		r, err = volumes.Inspect(context.Background(), &api.SdkVolumeInspectRequest{
			VolumeId: id,
		})
		assert.Error(t, err)

		tester.Stop()
	}
}
