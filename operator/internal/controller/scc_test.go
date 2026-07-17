package controller

import "testing"

func TestSCCDefinitions(t *testing.T) {
	const ns = "custom-ns"
	defs := sccDefinitions(ns)

	if len(defs) != 2 {
		t.Fatalf("expected 2 SCC definitions, got %d", len(defs))
	}

	byName := map[string]sccDefinition{}
	for _, d := range defs {
		byName[d.name] = d
	}

	node, ok := byName[NodeSCCName]
	if !ok {
		t.Fatalf("missing node SCC %q", NodeSCCName)
	}
	if node.fields["allowPrivilegedContainer"] != true {
		t.Errorf("node SCC allowPrivilegedContainer = %v, want true", node.fields["allowPrivilegedContainer"])
	}
	assertSCCUser(t, node, ns, NodeServiceAccount)
	assertRunAsAny(t, node)

	ctrl, ok := byName[ControllerSCCName]
	if !ok {
		t.Fatalf("missing controller SCC %q", ControllerSCCName)
	}
	if ctrl.fields["allowPrivilegedContainer"] != false {
		t.Errorf("controller SCC allowPrivilegedContainer = %v, want false", ctrl.fields["allowPrivilegedContainer"])
	}
	assertSCCUser(t, ctrl, ns, ControllerServiceAccount)
	assertRunAsAny(t, ctrl)
}

func assertSCCUser(t *testing.T, def sccDefinition, namespace, sa string) {
	t.Helper()
	want := "system:serviceaccount:" + namespace + ":" + sa
	users, ok := def.fields["users"].([]any)
	if !ok || len(users) != 1 {
		t.Fatalf("%s users = %v, want a single entry", def.name, def.fields["users"])
	}
	if users[0] != want {
		t.Errorf("%s user = %v, want %q", def.name, users[0], want)
	}
}

func assertRunAsAny(t *testing.T, def sccDefinition) {
	t.Helper()
	rau, ok := def.fields["runAsUser"].(map[string]any)
	if !ok || rau["type"] != "RunAsAny" {
		t.Errorf("%s runAsUser = %v, want type RunAsAny", def.name, def.fields["runAsUser"])
	}
}
