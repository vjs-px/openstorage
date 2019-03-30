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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/sirupsen/logrus"

	"github.com/libopenstorage/openstorage/api/server"
	"github.com/libopenstorage/openstorage/api/server/sdk"
	"github.com/libopenstorage/openstorage/cluster"
	clustermanager "github.com/libopenstorage/openstorage/cluster/manager"
	"github.com/libopenstorage/openstorage/config"
	"github.com/libopenstorage/openstorage/pkg/auth"
	sdkauth "github.com/libopenstorage/openstorage/pkg/auth"
	osecrets "github.com/libopenstorage/openstorage/pkg/auth/secrets"
	"github.com/libopenstorage/openstorage/pkg/grpcserver"
	"github.com/libopenstorage/openstorage/pkg/role"
	"github.com/libopenstorage/openstorage/pkg/storagepolicy"
	volumedrivers "github.com/libopenstorage/openstorage/volume/drivers"

	"github.com/stretchr/testify/assert"

	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/mem"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	defaultRestPort    = "8195"
	defaultSdkPort     = "8196"
	defaultSdkRestPort = "8197"
	defaultSdkSock     = "/tmp/sdk.sock"
)

const (
	testerKvDomain = "tester"
)

type Tester struct {
	t             *testing.T
	sdkServer     *sdk.Server
	restServer    *http.Server
	restUdsServer *http.Server
	conn          *grpc.ClientConn
	cm            cluster.Cluster
	kv            kvdb.Kvdb
	config        TesterConfig
	sdkPort       string
	sdkRestPort   string
	sdkSock       string
	restPort      string
}

type TesterConfig struct {
	SharedSecret string
	TokenIssuer  string
	RestPort     string
	SdkPort      string
	SdkRestPort  string
	SdkSock      string
}

func setConfigValue(v, d string) string {
	if len(v) != 0 {
		return v
	}
	return d
}

func New(t *testing.T, c *TesterConfig) *Tester {
	if c == nil || t == nil {
		t.Fatalf("Cannot pass nil as an argumet")
	}

	// Check config
	if len(c.SharedSecret) != 0 && len(c.TokenIssuer) == 0 {
		t.Fatalf("Cannot start osd tester server. Passing secret without issuer")
	}

	// Setup Kvdb
	kv, err := kvdb.New(mem.Name, testerKvDomain, []string{}, nil, logrus.Panicf)
	if err != nil {
		logrus.Panicf("Failed to initialize KVDB")
	}
	if err := kvdb.SetInstance(kv); err != nil {
		logrus.Panicf("Failed to set KVDB instance")
	}

	// Need to setup a fake cluster. No need to start it.
	clustermanager.Init(config.ClusterConfig{
		ClusterId: "fakecluster",
		NodeId:    "fakeNode",
	})

	cm, err := clustermanager.Inst()
	if err != nil {
		logrus.Panicf("Unable to initialize cluster manager: %v", err)
	}

	// Requires a non-nil cluster
	if err := volumedrivers.Register("fake", map[string]string{}); err != nil {
		logrus.Panicf("Unable to start volume driver fake: %v", err)
	}

	return &Tester{
		cm:          cm,
		kv:          kv,
		config:      *c,
		t:           t,
		sdkPort:     setConfigValue(c.SdkPort, defaultSdkPort),
		restPort:    setConfigValue(c.RestPort, defaultRestPort),
		sdkRestPort: setConfigValue(c.SdkRestPort, defaultSdkRestPort),
		sdkSock:     setConfigValue(c.SdkSock, defaultSdkSock),
	}
}

func (s *Tester) UrlRest() string {
	return "http://localhost:" + s.restPort
}

func (s *Tester) UrlSdkRest() string {
	return "http://localhost:" + s.sdkRestPort
}

func (s *Tester) SdkEndpoint() string {
	return "localhost:" + s.sdkPort
}

func (s *Tester) SdkUds() string {
	return s.sdkSock
}

func (s *Tester) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *Tester) Start() {
	// Start storage policy manager
	storagepolicy.Init(s.kv)
	stp, err := storagepolicy.Inst()
	assert.NoError(s.t, err)

	// clean up any left over uds
	os.Remove(s.sdkSock)

	// create the sdk
	sdkConfig := &sdk.ServerConfig{
		DriverName:    "fake",
		Net:           "tcp",
		Address:       ":" + s.sdkPort,
		RestPort:      s.sdkRestPort,
		StoragePolicy: stp,
		Cluster:       s.cm,
		Socket:        s.sdkSock,
		AccessOutput:  ioutil.Discard,
		AuditOutput:   ioutil.Discard,
	}

	// Check for authentication
	if len(s.config.SharedSecret) != 0 {
		rm, err := role.NewSdkRoleManager(s.kv)
		assert.NoError(s.t, err)

		jwtauth, err := auth.NewJwtAuth(&auth.JwtAuthConfig{
			SharedSecret: []byte(s.config.SharedSecret),
		})
		assert.NoError(s.t, err)

		sdkConfig.Security = &sdk.SecurityConfig{
			Role: rm,
			Authenticators: map[string]auth.Authenticator{
				s.config.TokenIssuer: jwtauth,
			},
		}
	}

	// Create SDK
	s.sdkServer, err = sdk.New(sdkConfig)
	assert.Nil(s.t, err)

	// Start SDK
	err = s.sdkServer.Start()
	assert.Nil(s.t, err)

	// setup deprecated REST server
	mgmtPort, err := strconv.ParseUint(s.restPort, 10, 16)
	s.restUdsServer, s.restServer, err = server.StartVolumeMgmtAPI(
		"fake", s.sdkSock,
		"/tmp",
		uint16(mgmtPort),
		false,
		osecrets.TypeNone, nil,
	)

	// Setup a connection to the driver
	s.conn, err = grpcserver.Connect("localhost:"+s.sdkPort, []grpc.DialOption{grpc.WithInsecure()})
	assert.Nil(s.t, err)
}

func (s *Tester) Stop() {
	s.conn.Close()
	s.sdkServer.Stop()
	s.restServer.Close()
	s.restUdsServer.Close()
	s.ClearKvdb()
	os.Remove(s.sdkSock)
}

func (s *Tester) ClearKvdb() {
	k, _ := s.kv.Serialize()
	fmt.Printf("KV(before): %s\n", string(k))
	s.kv.DeleteTree("")
	k, _ = s.kv.Serialize()
	fmt.Printf("KV(after): %s\n", string(k))
}

func (s *Tester) CreateTestToken(name, role, secret string) (string, error) {
	claims := &sdkauth.Claims{
		Issuer: s.config.TokenIssuer,
		Name:   name,
		Email:  name + "@openstorage.org",
		Roles:  []string{role},
	}
	signature := &sdkauth.Signature{
		Key:  []byte(secret),
		Type: jwt.SigningMethodHS256,
	}
	options := &sdkauth.Options{
		Expiration: time.Now().Add(1 * time.Hour).Unix(),
	}
	return sdkauth.Token(claims, signature, options)
}

func (s *Tester) ContextWithToken(ctx context.Context, name, role, secret string) (context.Context, error) {
	token, err := s.CreateTestToken(name, role, secret)
	if err != nil {
		return nil, err
	}
	md := metadata.New(map[string]string{
		"authorization": "bearer " + token,
	})
	return metadata.NewOutgoingContext(ctx, md), nil
}
