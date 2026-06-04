package client

import (
	"encoding/json"
	"errors"
	"testing"
)

// firstParamMap unmarshals the first positional parameter of the first recorded
// request for method into a map, for asserting on the JSON payload sent.
func firstParamMap(t *testing.T, mock *MockTrueNASServer, method string) map[string]any {
	t.Helper()
	reqs := mock.GetRequestsByMethod(method)
	if len(reqs) == 0 {
		t.Fatalf("no recorded request for %s", method)
	}
	var params []json.RawMessage
	if err := json.Unmarshal(reqs[0].Params, &params); err != nil {
		t.Fatalf("unmarshal params for %s: %v", method, err)
	}
	if len(params) == 0 {
		t.Fatalf("no positional params for %s", method)
	}
	var m map[string]any
	if err := json.Unmarshal(params[0], &m); err != nil {
		t.Fatalf("unmarshal first param for %s: %v", method, err)
	}
	return m
}

func TestGetNVMeGlobalConfig_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetGlobalConfig, MockResponse{
		Result: NVMeGlobalConfig{ID: 1, BaseNQN: "nqn.2011-06.com.truenas", Kernel: true},
	})

	client := connectTestClient(t, mock)

	cfg, err := client.GetNVMeGlobalConfig(testContext(t))
	assertNoError(t, err)
	assertNotNil(t, cfg)
	assertEqual(t, cfg.BaseNQN, "nqn.2011-06.com.truenas")
	assertTrue(t, cfg.Kernel)
}

func TestCreateNVMeSubsystem_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetSubsysCreate, MockResponse{
		Result: NVMeSubsystem{
			ID:           7,
			Name:         "csi-pvc-abc",
			SubNQN:       "nqn.2011-06.com.truenas:csi-pvc-abc",
			AllowAnyHost: true,
		},
	})

	client := connectTestClient(t, mock)

	subsys, err := client.CreateNVMeSubsystem(testContext(t), "csi-pvc-abc", true)
	assertNoError(t, err)
	assertNotNil(t, subsys)
	assertEqual(t, subsys.ID, 7)
	// subnqn is read back from the server, not supplied.
	assertEqual(t, subsys.SubNQN, "nqn.2011-06.com.truenas:csi-pvc-abc")

	// Verify the create payload: name + allow_any_host sent, subnqn omitted.
	params := firstParamMap(t, mock, methodNVMetSubsysCreate)
	assertEqual(t, params["name"], "csi-pvc-abc")
	if allow, ok := params["allow_any_host"].(bool); !ok || !allow {
		t.Errorf("expected allow_any_host=true in payload, got %v", params["allow_any_host"])
	}
	if _, present := params["subnqn"]; present {
		t.Error("subnqn must be omitted on create (server generates it)")
	}
}

func TestGetNVMeSubsystemByNQN_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetSubsysQuery, MockResponse{
		Result: []NVMeSubsystem{{ID: 3, Name: "s", SubNQN: "nqn.x:csi-vol"}},
	})

	client := connectTestClient(t, mock)

	subsys, err := client.GetNVMeSubsystemByNQN(testContext(t), "nqn.x:csi-vol")
	assertNoError(t, err)
	assertNotNil(t, subsys)
	assertEqual(t, subsys.ID, 3)
}

func TestGetNVMeSubsystemByNQN_NotFound(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetSubsysQuery, MockResponse{Result: []NVMeSubsystem{}})

	client := connectTestClient(t, mock)

	subsys, err := client.GetNVMeSubsystemByNQN(testContext(t), "nqn.x:missing")
	assertError(t, err)
	assertNil(t, subsys)
	assertTrue(t, errors.Is(err, ErrNotFound))
}

func TestCreateNVMeNamespace_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetNamespaceCreate, MockResponse{
		Result: NVMeNamespace{
			ID:         4,
			NSID:       1,
			DeviceType: NVMeDeviceTypeZVOL,
			DevicePath: "zvol/tank/pvc-abc",
			DeviceUUID: "e1f2a3b4-0000-1111-2222-333344445555",
			Enabled:    true,
			Subsys:     &NVMeSubsystem{ID: 7, SubNQN: "nqn.x:csi-pvc-abc"},
		},
	})

	client := connectTestClient(t, mock)

	ns, err := client.CreateNVMeNamespace(testContext(t), 7, NVMeDeviceTypeZVOL, "zvol/tank/pvc-abc")
	assertNoError(t, err)
	assertNotNil(t, ns)
	assertEqual(t, ns.ID, 4)
	assertEqual(t, ns.DeviceUUID, "e1f2a3b4-0000-1111-2222-333344445555")
	assertNotNil(t, ns.Subsys)
	assertEqual(t, ns.Subsys.SubNQN, "nqn.x:csi-pvc-abc")

	params := firstParamMap(t, mock, methodNVMetNamespaceCreate)
	assertEqual(t, params["device_type"], "ZVOL")
	assertEqual(t, params["device_path"], "zvol/tank/pvc-abc")
	if enabled, ok := params["enabled"].(bool); !ok || !enabled {
		t.Errorf("expected enabled=true, got %v", params["enabled"])
	}
}

func TestGetNVMeNamespaceByDevice_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetNamespaceQuery, MockResponse{
		Result: []NVMeNamespace{{
			ID:         4,
			DevicePath: "zvol/tank/pvc-abc",
			Subsys:     &NVMeSubsystem{ID: 7, SubNQN: "nqn.x:csi-pvc-abc"},
		}},
	})

	client := connectTestClient(t, mock)

	ns, err := client.GetNVMeNamespaceByDevice(testContext(t), "zvol/tank/pvc-abc")
	assertNoError(t, err)
	assertNotNil(t, ns)
	assertEqual(t, ns.DevicePath, "zvol/tank/pvc-abc")
	assertNotNil(t, ns.Subsys)
	assertEqual(t, ns.Subsys.SubNQN, "nqn.x:csi-pvc-abc")
}

func TestGetNVMeNamespacesBySubsystem_FiltersClientSide(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetNamespaceQuery, MockResponse{
		Result: []NVMeNamespace{
			{ID: 1, DevicePath: "zvol/tank/a", Subsys: &NVMeSubsystem{ID: 7}},
			{ID: 2, DevicePath: "zvol/tank/b", Subsys: &NVMeSubsystem{ID: 9}},
			{ID: 3, DevicePath: "zvol/tank/c", Subsys: &NVMeSubsystem{ID: 7}},
		},
	})

	client := connectTestClient(t, mock)

	got, err := client.GetNVMeNamespacesBySubsystem(testContext(t), 7)
	assertNoError(t, err)
	assertLen(t, got, 2)
	for _, ns := range got {
		assertEqual(t, ns.Subsys.ID, 7)
	}
}

func TestCreateNVMePort_AndGetByAddr(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetPortCreate, MockResponse{
		Result: NVMePort{ID: 1, AddrTrType: NVMeTransportTCP, AddrTrAddr: "10.0.0.136", AddrTrSvcID: 4420},
	})
	mock.SetResponse(methodNVMetPortQuery, MockResponse{
		Result: []NVMePort{
			{ID: 9, AddrTrType: "RDMA", AddrTrAddr: "10.0.0.136"},
			{ID: 1, AddrTrType: NVMeTransportTCP, AddrTrAddr: "10.0.0.136", AddrTrSvcID: 4420},
		},
	})

	client := connectTestClient(t, mock)

	port, err := client.CreateNVMePort(testContext(t), "10.0.0.136", NVMeDefaultPort)
	assertNoError(t, err)
	assertNotNil(t, port)
	assertEqual(t, port.ID, 1)

	params := firstParamMap(t, mock, methodNVMetPortCreate)
	assertEqual(t, params["addr_trtype"], "TCP")
	assertEqual(t, params["addr_traddr"], "10.0.0.136")
	if _, present := params["addr_adrfam"]; present {
		t.Error("addr_adrfam must not be sent on create (server-derived)")
	}

	// GetByAddr should skip the RDMA entry and return the TCP one.
	found, err := client.GetNVMePortByAddr(testContext(t), "10.0.0.136")
	assertNoError(t, err)
	assertNotNil(t, found)
	assertEqual(t, found.AddrTrType, "TCP")
	assertEqual(t, found.ID, 1)
}

func TestCreateNVMePortSubsys_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetPortSubsysCreate, MockResponse{
		Result: NVMePortSubsys{ID: 2, Port: &NVMePort{ID: 1}, Subsys: &NVMeSubsystem{ID: 7}},
	})

	client := connectTestClient(t, mock)

	ps, err := client.CreateNVMePortSubsys(testContext(t), 1, 7)
	assertNoError(t, err)
	assertNotNil(t, ps)
	assertEqual(t, ps.ID, 2)

	params := firstParamMap(t, mock, methodNVMetPortSubsysCreate)
	assertEqual(t, params["port_id"].(float64), float64(1))
	assertEqual(t, params["subsys_id"].(float64), float64(7))
}

func TestGetNVMePortSubsysBySubsystem_FiltersClientSide(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetPortSubsysQuery, MockResponse{
		Result: []NVMePortSubsys{
			{ID: 1, Subsys: &NVMeSubsystem{ID: 7}},
			{ID: 2, Subsys: &NVMeSubsystem{ID: 8}},
		},
	})

	client := connectTestClient(t, mock)

	got, err := client.GetNVMePortSubsysBySubsystem(testContext(t), 7)
	assertNoError(t, err)
	assertLen(t, got, 1)
	assertEqual(t, got[0].ID, 1)
}

func TestCreateNVMeHost_WithDHCHAP(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetHostCreate, MockResponse{
		Result: NVMeHost{ID: 5, HostNQN: "nqn.2014-08.org.nvmexpress:uuid:node-a"},
	})

	client := connectTestClient(t, mock)

	host, err := client.CreateNVMeHost(testContext(t), &NVMeHostCreateOptions{
		HostNQN:    "nqn.2014-08.org.nvmexpress:uuid:node-a",
		DHCHAPKey:  "DHHC-1:01:abc:",
		DHCHAPHash: "SHA-256",
	})
	assertNoError(t, err)
	assertNotNil(t, host)
	assertEqual(t, host.ID, 5)

	params := firstParamMap(t, mock, methodNVMetHostCreate)
	assertEqual(t, params["hostnqn"], "nqn.2014-08.org.nvmexpress:uuid:node-a")
	assertEqual(t, params["dhchap_key"], "DHHC-1:01:abc:")
	assertEqual(t, params["dhchap_hash"], "SHA-256")
}

func TestGetNVMeHostByNQN_Success(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetHostQuery, MockResponse{
		Result: []NVMeHost{{ID: 5, HostNQN: "nqn.x:node-a"}},
	})

	client := connectTestClient(t, mock)

	host, err := client.GetNVMeHostByNQN(testContext(t), "nqn.x:node-a")
	assertNoError(t, err)
	assertNotNil(t, host)
	assertEqual(t, host.ID, 5)
}

func TestGetNVMeHostSubsysByHost_FiltersForGC(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	// Host 5 is linked to two subsystems; deleting one volume must not orphan the host.
	mock.SetResponse(methodNVMetHostSubsysQuery, MockResponse{
		Result: []NVMeHostSubsys{
			{ID: 1, Host: &NVMeHost{ID: 5}, Subsys: &NVMeSubsystem{ID: 7}},
			{ID: 2, Host: &NVMeHost{ID: 6}, Subsys: &NVMeSubsystem{ID: 8}},
			{ID: 3, Host: &NVMeHost{ID: 5}, Subsys: &NVMeSubsystem{ID: 9}},
		},
	})

	client := connectTestClient(t, mock)

	got, err := client.GetNVMeHostSubsysByHost(testContext(t), 5)
	assertNoError(t, err)
	assertLen(t, got, 2)
}

func TestGetNVMeHostSubsysBySubsystem_FiltersClientSide(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	mock.SetResponse(methodNVMetHostSubsysQuery, MockResponse{
		Result: []NVMeHostSubsys{
			{ID: 1, Host: &NVMeHost{ID: 5}, Subsys: &NVMeSubsystem{ID: 7}},
			{ID: 2, Host: &NVMeHost{ID: 6}, Subsys: &NVMeSubsystem{ID: 7}},
			{ID: 3, Host: &NVMeHost{ID: 5}, Subsys: &NVMeSubsystem{ID: 9}},
		},
	})

	client := connectTestClient(t, mock)

	got, err := client.GetNVMeHostSubsysBySubsystem(testContext(t), 7)
	assertNoError(t, err)
	assertLen(t, got, 2)
}

func TestDeleteNVMeResources_SendID(t *testing.T) {
	mock := NewMockTrueNASServer()
	defer mock.Close()

	client := connectTestClient(t, mock)
	ctx := testContext(t)

	assertNoError(t, client.DeleteNVMeNamespace(ctx, 4))
	assertNoError(t, client.DeleteNVMeSubsystem(ctx, 7))
	assertNoError(t, client.DeleteNVMePortSubsys(ctx, 2))
	assertNoError(t, client.DeleteNVMeHostSubsys(ctx, 3))
	assertNoError(t, client.DeleteNVMeHost(ctx, 5))

	// Verify the subsystem delete sent the id as the first positional param.
	reqs := mock.GetRequestsByMethod(methodNVMetSubsysDelete)
	assertLen(t, reqs, 1)
	var params []any
	assertNoError(t, json.Unmarshal(reqs[0].Params, &params))
	assertLen(t, params, 1)
	assertEqual(t, params[0].(float64), float64(7))
}
