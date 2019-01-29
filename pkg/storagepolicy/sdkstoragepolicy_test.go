package storagepolicy

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/jsonpb"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/mem"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPrefixWithName(t *testing.T) {
	assert.Equal(t, prefixWithName("H$ll0_123$"), policyPrefix+"/"+"H$ll0_123$")
}

func TestSdkStoragePolicyCreate(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
		SharedOpt: &api.VolumeSpecUpdate_Shared{
			Shared: false,
		},
		Sharedv4Opt: &api.VolumeSpecUpdate_Sharedv4{
			Sharedv4: false,
		},
		JournalOpt: &api.VolumeSpecUpdate_Journal{
			Journal: true,
		},
		HaLevelOpt: &api.VolumeSpecUpdate_HaLevel{
			HaLevel: 3,
		},
		SnapshotScheduleOpt: &api.VolumeSpecUpdate_SnapshotSchedule{
			SnapshotSchedule: "freq:periodic\nperiod:120000\n",
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testbasicpolicy",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	// Assert the information is in kvdb
	var policy *api.VolumeSpecUpdate
	kvp, err := kv.GetVal(prefixWithName("testbasicpolicy"), &policy)
	assert.NoError(t, err)

	err = jsonpb.Unmarshal(strings.NewReader(string(kvp.Value)), policy)
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(policy, req.StoragePolicy.GetPolicy()))
}

func TestSdkStoragePolicyCreateBadArguments(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)
	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)

	// empty params
	req := &api.SdkOpenStoragePolicyCreateRequest{}
	_, err = s.Create(context.Background(), req)
	assert.Error(t, err)
	serverError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)

	// empty vol specs
	req = &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name: "testbasicpolicy",
		},
	}
	_, err = s.Create(context.Background(), req)
	assert.Error(t, err)
	serverError, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)
	assert.Contains(t, serverError.Message(), "Must supply Volume Specs")
}

func TestSdkStoragePolicyInspect(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 2000,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "Test_In$pect-123",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{
		Name: "Test_In$pect-123",
	}

	resp, err := s.Inspect(context.Background(), inspReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.StoragePolicy.GetName(), inspReq.GetName())
	assert.True(t, reflect.DeepEqual(resp.StoragePolicy.GetPolicy(), req.StoragePolicy.GetPolicy()))
}

func TestSdkStoragePolicyInspectBadArgument(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testinspect",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{}
	_, err = s.Inspect(context.Background(), inspReq)
	serverError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)
	assert.Contains(t, serverError.Message(), "Must supply a Storage Policy Name")

	inspReq = &api.SdkOpenStoragePolicyInspectRequest{
		Name: "non-existent-name",
	}
	_, err = s.Inspect(context.Background(), inspReq)
	serverError, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.NotFound)
	assert.Contains(t, serverError.Message(), "not found")
}

func TestSdkStoragePolicyUpdate(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testupdate",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{
		Name: "testupdate",
	}

	resp, err := s.Inspect(context.Background(), inspReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.StoragePolicy.GetName(), inspReq.GetName())
	assert.True(t, reflect.DeepEqual(resp.StoragePolicy.GetPolicy(), req.StoragePolicy.GetPolicy()))

	// update volume
	updateSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 4896,
		},
	}

	updateReq := &api.SdkOpenStoragePolicyUpdateRequest{
		Name: "testupdate",
		StoragePolicy: &api.SdkStoragePolicy{
			Policy: updateSpec,
		},
	}

	_, err = s.Update(context.Background(), updateReq)
	assert.NoError(t, err)

	resp, err = s.Inspect(context.Background(), inspReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.StoragePolicy.GetName(), inspReq.GetName())
	assert.True(t, reflect.DeepEqual(resp.StoragePolicy.GetPolicy(), updateReq.StoragePolicy.GetPolicy()))

}

func TestSdkStoragePolicyUpdateBadArgument(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testinspect",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{}
	_, err = s.Inspect(context.Background(), inspReq)
	serverError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)
	assert.Contains(t, serverError.Message(), "Must supply a Storage Policy Name")

	updateReq := &api.SdkOpenStoragePolicyUpdateRequest{
		Name: "testinspect",
	}
	_, err = s.Update(context.Background(), updateReq)
	serverError, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)
	assert.Contains(t, serverError.Message(), "Must supply Volume Specs")

	updateReq = &api.SdkOpenStoragePolicyUpdateRequest{
		Name: "non-existant-key",
		StoragePolicy: &api.SdkStoragePolicy{
			Policy: volSpec,
		},
	}
	_, err = s.Update(context.Background(), updateReq)
	serverError, ok = status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.NotFound)
	assert.Contains(t, serverError.Message(), "not found")
}
func TestSdkStoragePolicyDelete(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testdelete",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{
		Name: "testdelete",
	}

	resp, err := s.Inspect(context.Background(), inspReq)
	assert.NoError(t, err)
	assert.Equal(t, resp.StoragePolicy.GetName(), inspReq.GetName())
	assert.True(t, reflect.DeepEqual(resp.StoragePolicy.GetPolicy(), req.StoragePolicy.GetPolicy()))

	delReq := &api.SdkOpenStoragePolicyDeleteRequest{
		Name: "testdelete",
	}
	_, err = s.Delete(context.Background(), delReq)
	assert.NoError(t, err)

	resp, err = s.Inspect(context.Background(), inspReq)
	serverError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.NotFound)
	assert.Contains(t, serverError.Message(), "not found")
}

func TestSdkStoragePolicyDeleteBadArgument(t *testing.T) {
	kv, err := kvdb.New(mem.Name, "policy", []string{}, nil, logrus.Panicf)
	assert.NoError(t, err)

	s, err := NewSdkStoragePolicyManager(kv)
	assert.NoError(t, err)

	// Create Policy
	volSpec := &api.VolumeSpecUpdate{
		SizeOpt: &api.VolumeSpecUpdate_Size{
			Size: 1234,
		},
	}

	req := &api.SdkOpenStoragePolicyCreateRequest{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   "testdelete",
			Policy: volSpec,
		},
	}

	_, err = s.Create(context.Background(), req)
	assert.NoError(t, err)

	inspReq := &api.SdkOpenStoragePolicyInspectRequest{
		Name: "testdelete",
	}
	_, err = s.Inspect(context.Background(), inspReq)
	assert.NoError(t, err)

	// empty policy name
	delReq := &api.SdkOpenStoragePolicyDeleteRequest{}
	_, err = s.Delete(context.Background(), delReq)
	serverError, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, serverError.Code(), codes.InvalidArgument)
	assert.Contains(t, serverError.Message(), "Must supply a Storage Policy Name")
}
