package storagepolicy

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/portworx/kvdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/libopenstorage/openstorage/api"
)

// SdkPolicyManager is an implementation of the Storage Policy Manager for the SDK
type SdkPolicyManager struct {
	kv kvdb.Kvdb
}

const (
	policyPrefix = "/storage/policy"
)

// Check interface
var _ PolicyManager = &SdkPolicyManager{}

// Simple function which creates key for Kvdb
func prefixWithName(name string) string {
	return policyPrefix + "/" + name
}

// NewSdkPolicyManager returns a new SDK Storage Policy Manager
func NewSdkStoragePolicyManager(kv kvdb.Kvdb) (*SdkPolicyManager, error) {
	s := &SdkPolicyManager{
		kv: kv,
	}

	return s, nil
}

func (p *SdkPolicyManager) Create(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyCreateRequest,
) (*api.SdkOpenStoragePolicyCreateResponse, error) {
	if req.StoragePolicy.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	if req.StoragePolicy.GetPolicy() == nil {
		return nil, status.Error(codes.InvalidArgument, "Must supply Volume Specs")
	}

	kvp, err := p.kv.Create(prefixWithName(req.StoragePolicy.GetName()), req.StoragePolicy.GetPolicy(), 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to save storage policy: %v", err)
	}
	var s *api.VolumeSpecUpdate
	json.Unmarshal(kvp.Value, &s)
	fmt.Printf("KVP %v", s)

	return &api.SdkOpenStoragePolicyCreateResponse{}, nil
}

func (p *SdkPolicyManager) Update(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyUpdateRequest,
) (*api.SdkOpenStoragePolicyUpdateResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	if req.StoragePolicy.GetPolicy() == nil {
		return nil, status.Error(codes.InvalidArgument, "Must supply Volume Specs")
	}

	_, err := p.kv.Update(prefixWithName(req.StoragePolicy.GetName()), req.StoragePolicy.GetPolicy(), 0)
	if err == kvdb.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "Storage Policy %s not found", req.StoragePolicy.GetPolicy())
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update storage policy: %v", err)
	}

	return &api.SdkOpenStoragePolicyUpdateResponse{}, nil
}

func (p *SdkPolicyManager) Delete(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyDeleteRequest,
) (*api.SdkOpenStoragePolicyDeleteResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	_, err := p.kv.Delete(prefixWithName(req.GetName()))
	if err != kvdb.ErrNotFound && err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete Storage Policy %s: %v", req.GetName(), err)
	}

	return &api.SdkOpenStoragePolicyDeleteResponse{}, nil
}

func (p *SdkPolicyManager) Inspect(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyInspectRequest,
) (*api.SdkOpenStoragePolicyInspectResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	var policy *api.SdkStoragePolicy
	_, err := p.kv.GetVal(prefixWithName(req.GetName()), &policy)
	if err == kvdb.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "Role %s not found", req.GetName())
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get role %s information: %v", req.GetName(), err)
	}

	return &api.SdkOpenStoragePolicyInspectResponse{
		StoragePolicy: policy,
	}, nil
}

func (p *SdkPolicyManager) Enumerate(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyEnumerateRequest,
) (*api.SdkOpenStoragePolicyEnumerateResponse, error) {
	// TODO: check whether this is actually required for phase 1
	return &api.SdkOpenStoragePolicyEnumerateResponse{}, nil
}

func (p *SdkPolicyManager) SetEnforcement(name string) error {
	return nil
}

func (p *SdkPolicyManager) DisableEnforcement() error {
	return nil
}
