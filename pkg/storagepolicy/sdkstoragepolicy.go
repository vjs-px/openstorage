package storagepolicy

import (
	"context"
	"strings"

	"github.com/portworx/kvdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/libopenstorage/openstorage/api"
	"github.com/libopenstorage/openstorage/pkg/jsonpb"
)

// SdkPolicyManager is an implementation of the Storage Policy Manager for the SDK
type SdkPolicyManager struct {
	kv kvdb.Kvdb
}

const (
	policyPrefix = "/storage/policy"
	EnforcePath  = "/storage/enforce"
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

	// Since VolumeSpecUpdate has oneof method of proto, we need to marshal it into string using protobuf
	// jsonpb
	m := jsonpb.Marshaler{}
	policyStr, err := m.MarshalToString(req.StoragePolicy.GetPolicy())

	_, err = p.kv.Create(prefixWithName(req.StoragePolicy.GetName()), policyStr, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to save storage policy: %v", err)
	}

	return &api.SdkOpenStoragePolicyCreateResponse{}, nil
}

func (p *SdkPolicyManager) Update(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyUpdateRequest,
) (*api.SdkOpenStoragePolicyUpdateResponse, error) {
	if req.StoragePolicy.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	if req.StoragePolicy.GetPolicy() == nil {
		return nil, status.Error(codes.InvalidArgument, "Must supply Volume Specs")
	}

	m := jsonpb.Marshaler{}
	policyStr, err := m.MarshalToString(req.StoragePolicy.GetPolicy())

	_, err = p.kv.Update(prefixWithName(req.StoragePolicy.GetName()), policyStr, 0)
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

	var volSpecs *api.VolumeSpecUpdate
	kvp, err := p.kv.GetVal(prefixWithName(req.GetName()), &volSpecs)
	if err == kvdb.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "Policy %s not found", req.GetName())
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get policy %s information: %v", req.GetName(), err)
	}

	err = jsonpb.Unmarshal(strings.NewReader(string(kvp.Value)), volSpecs)
	if err != nil {
		return nil, err
	}

	return &api.SdkOpenStoragePolicyInspectResponse{
		StoragePolicy: &api.SdkStoragePolicy{
			Name:   req.GetName(),
			Policy: volSpecs,
		},
	}, nil
}

func (p *SdkPolicyManager) Enumerate(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyEnumerateRequest,
) (*api.SdkOpenStoragePolicyEnumerateResponse, error) {

	// get all keyValue pair at /storage/policy
	kvp, err := p.kv.Enumerate(policyPrefix)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get policies from database: %v", err)
	}

	policies := make([]*api.SdkStoragePolicy, 0)
	for _, policy := range kvp {
		volSpecs := &api.VolumeSpecUpdate{}
		err = jsonpb.Unmarshal(strings.NewReader(string(policy.Value)), volSpecs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Unable to parse %s policy: %v", policy.Key, err)
		}
		storagePolicy := &api.SdkStoragePolicy{
			Name:   strings.TrimPrefix(policy.Key, policyPrefix+"/"),
			Policy: volSpecs,
		}
		policies = append(policies, storagePolicy)
	}

	return &api.SdkOpenStoragePolicyEnumerateResponse{
		StoragePolicies: policies,
	}, nil
}

func (p *SdkPolicyManager) Enforce(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyEnforceRequest,
) (*api.SdkOpenStoragePolicyEnforceResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Must supply a Storage Policy Name")
	}

	policy, err := p.Inspect(ctx,
		&api.SdkOpenStoragePolicyInspectRequest{
			Name: req.GetName(),
		},
	)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "Policy with name %s not found", req.GetName())
	}

	m := jsonpb.Marshaler{}
	policyStr, err := m.MarshalToString(policy.StoragePolicy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Json marshal failed for policy %s :%v", req.GetName, err)
	}

	_, err = p.kv.Update(EnforcePath, policyStr, 0)
	if err == kvdb.ErrNotFound {
		if _, err := p.kv.Create(EnforcePath, policyStr, 0); err != nil {
			return nil, status.Errorf(codes.Internal, "Unable to save enforcement details %v", err)
		}
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to enforce policy: %v", err)
	}

	return &api.SdkOpenStoragePolicyEnforceResponse{}, nil
}

func (p *SdkPolicyManager) Release(
	ctx context.Context,
	req *api.SdkOpenStoragePolicyReleaseRequest,
) (*api.SdkOpenStoragePolicyReleaseResponse, error) {
	// empty represents no policy enforcement is enabled
	_, err := p.kv.Update(EnforcePath, api.SdkStoragePolicy{}, 0)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Disable enforcement failed with: %v", err)
	}

	return &api.SdkOpenStoragePolicyReleaseResponse{}, nil
}

func (p *SdkPolicyManager) GetEnforcement() (*api.SdkStoragePolicy, error) {
	var defaultPolicy *api.SdkStoragePolicy
	kvp, err := p.kv.GetVal(EnforcePath, &defaultPolicy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unable to retrive Enforcement details %v", err)
	}

	err = jsonpb.Unmarshal(strings.NewReader(string(kvp.Value)), defaultPolicy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Unable to retrive Enforcement details %v", err)
	}

	return defaultPolicy, nil
}
