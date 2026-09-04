// Command chartcheck compares the rendered Helm chart against the flat
// deployment manifest.
//
// The two install paths describe the same workloads, so anything that changes in
// one has to change in the other. That duplication is exactly how the node
// postStart hook came to carry the same wrong iscsiadm path twice, so this check
// exists to make the drift loud instead of latent.
//
// It compares the fields that break a deployment when they diverge: container
// images and arguments, environment variables and the ConfigMap or Secret keys
// they read, volumes and their host paths, mounts, lifecycle hooks, RBAC rules,
// and the CSIDriver spec. It deliberately ignores labels, annotations,
// namespaces, replica counts and the ConfigMap and Secret values themselves,
// since those are what the chart is meant to parameterise.
//
// Usage:
//
//	chartcheck <rendered-chart.yaml> <deploy-manifest.yaml>
package main

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// driverImageRepo is the driver image. Its tag is allowed to differ: the flat
// manifest tracks the floating :latest tag, while the chart pins the release it
// ships. The repository still has to match, and every sidecar image is compared
// in full.
const driverImageRepo = "ghcr.io/truenas/truenas-csi"

// comparedKinds are the kinds whose content must agree. Kinds absent from one
// side are reported as missing; kinds in neither list are ignored.
var comparedKinds = map[string]bool{
	"Deployment":         true,
	"DaemonSet":          true,
	"CSIDriver":          true,
	"ClusterRole":        true,
	"ClusterRoleBinding": true,
	"ConfigMap":          true,
	"Secret":             true,
	"ServiceAccount":     true,
}

// object is an alias, not a defined type: yaml.v3 decodes nested mappings using
// the enclosing map's own type, so a defined type here would make every
// map[string]any assertion below fail.
type object = map[string]any

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: chartcheck <rendered-chart.yaml> <deploy-manifest.yaml>")
		os.Exit(2)
	}

	chart, err := load(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading the rendered chart: %v\n", err)
		os.Exit(2)
	}
	manifest, err := load(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "reading the deploy manifest: %v\n", err)
		os.Exit(2)
	}

	var problems []string

	for _, key := range sortedKeys(manifest) {
		chartObj, ok := chart[key]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s is in the deploy manifest but not in the chart", key))
			continue
		}
		problems = append(problems, compare(key, normalize(key, chartObj), normalize(key, manifest[key]))...)
	}

	for _, key := range sortedKeys(chart) {
		if _, ok := manifest[key]; !ok {
			problems = append(problems, fmt.Sprintf("%s is in the chart but not in the deploy manifest", key))
		}
	}

	if len(problems) > 0 {
		fmt.Println("The Helm chart and deploy/truenas-csi-driver.yaml disagree:")
		for _, p := range problems {
			fmt.Printf("  - %s\n", p)
		}
		fmt.Println("\nUpdate whichever is stale, then re-run `make verify-chart`.")
		os.Exit(1)
	}

	fmt.Println("Chart and deploy manifest agree.")
}

// load reads a multi-document YAML file into objects keyed by kind and name.
func load(path string) (map[string]object, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	out := map[string]object{}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	for {
		var doc object
		err := decoder.Decode(&doc)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		if len(doc) == 0 {
			continue
		}
		kind, _ := doc["kind"].(string)
		if !comparedKinds[kind] {
			continue
		}
		meta, _ := doc["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		out[fmt.Sprintf("%s/%s", kind, name)] = doc
	}
	return out, nil
}

// normalize strips everything the chart is expected to parameterise, so what
// remains is the part both paths must state identically.
func normalize(key string, obj object) object {
	clone := deepCopy(obj).(object)
	delete(clone, "status")

	if meta, ok := clone["metadata"].(map[string]any); ok {
		delete(meta, "labels")
		delete(meta, "annotations")
		delete(meta, "namespace")
		delete(meta, "creationTimestamp")
	}

	switch {
	case strings.HasPrefix(key, "ConfigMap/"), strings.HasPrefix(key, "Secret/"):
		// Values are the whole point of the chart; only the set of keys matters,
		// because the workloads reference keys by name.
		for _, field := range []string{"data", "stringData"} {
			if data, ok := clone[field].(map[string]any); ok {
				clone[field] = keyNames(data)
			}
		}

	case strings.HasPrefix(key, "ClusterRoleBinding/"):
		if subjects, ok := clone["subjects"].([]any); ok {
			for _, s := range subjects {
				if subject, ok := s.(map[string]any); ok {
					delete(subject, "namespace")
				}
			}
		}

	case strings.HasPrefix(key, "Deployment/"), strings.HasPrefix(key, "DaemonSet/"):
		spec, _ := clone["spec"].(map[string]any)
		if spec == nil {
			break
		}
		// Replicas are an operator's choice, not a contract between the two paths.
		delete(spec, "replicas")
		template, _ := spec["template"].(map[string]any)
		if template == nil {
			break
		}
		if meta, ok := template["metadata"].(map[string]any); ok {
			delete(meta, "labels")
			delete(meta, "annotations")
			delete(meta, "creationTimestamp")
		}
		podSpec, _ := template["spec"].(map[string]any)
		if podSpec == nil {
			break
		}
		if containers, ok := podSpec["containers"].([]any); ok {
			for _, c := range containers {
				container, ok := c.(map[string]any)
				if !ok {
					continue
				}
				if image, ok := container["image"].(string); ok {
					container["image"] = normalizeImage(image)
				}
			}
		}
	}

	return clone
}

// normalizeImage drops the tag of the driver image only. Sidecar images keep
// their tags, which is the drift this check most needs to catch.
func normalizeImage(image string) string {
	if repo, _, found := strings.Cut(image, ":"); found && repo == driverImageRepo {
		return repo + ":<release>"
	}
	return image
}

// compare walks both objects and reports the paths whose values differ.
func compare(key string, chart, manifest object) []string {
	var problems []string
	diff(key, "", chart, manifest, &problems)
	return problems
}

func diff(key, path string, chart, manifest any, problems *[]string) {
	if reflect.DeepEqual(chart, manifest) {
		return
	}

	chartMap, chartIsMap := chart.(map[string]any)
	manifestMap, manifestIsMap := manifest.(map[string]any)
	if chartIsMap && manifestIsMap {
		seen := map[string]bool{}
		for _, k := range append(mapKeys(chartMap), mapKeys(manifestMap)...) {
			if seen[k] {
				continue
			}
			seen[k] = true
			chartVal, inChart := chartMap[k]
			manifestVal, inManifest := manifestMap[k]
			switch {
			case !inChart:
				*problems = append(*problems, fmt.Sprintf("%s%s: %q is only in the deploy manifest", key, join(path, k), render(manifestVal)))
			case !inManifest:
				*problems = append(*problems, fmt.Sprintf("%s%s: %q is only in the chart", key, join(path, k), render(chartVal)))
			default:
				diff(key, join(path, k), chartVal, manifestVal, problems)
			}
		}
		return
	}

	chartList, chartIsList := chart.([]any)
	manifestList, manifestIsList := manifest.([]any)
	if chartIsList && manifestIsList {
		// Containers, volumes and env vars are all name-keyed lists, so compare
		// them by name rather than by position.
		if named(chartList) && named(manifestList) {
			diff(key, path, byName(chartList), byName(manifestList), problems)
			return
		}
		if len(chartList) != len(manifestList) {
			*problems = append(*problems, fmt.Sprintf("%s%s: chart has %d entries, deploy manifest has %d", key, path, len(chartList), len(manifestList)))
			return
		}
		for i := range chartList {
			diff(key, fmt.Sprintf("%s[%d]", path, i), chartList[i], manifestList[i], problems)
		}
		return
	}

	*problems = append(*problems, fmt.Sprintf("%s%s: chart has %q, deploy manifest has %q", key, path, render(chart), render(manifest)))
}

// named reports whether every entry in a list is a map carrying a name, which
// makes the list safe to compare as a keyed set.
func named(list []any) bool {
	if len(list) == 0 {
		return false
	}
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return false
		}
		if _, ok := m["name"].(string); !ok {
			return false
		}
	}
	return true
}

func byName(list []any) map[string]any {
	out := map[string]any{}
	for _, item := range list {
		m := item.(map[string]any)
		out[m["name"].(string)] = m
	}
	return out
}

func keyNames(m map[string]any) []any {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]any, 0, len(names))
	for _, n := range names {
		out = append(out, n)
	}
	return out
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeys(m map[string]object) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func join(path, key string) string {
	return path + "." + key
}

func render(v any) string {
	out, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return strings.TrimSpace(string(out))
}

func deepCopy(v any) any {
	switch value := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(value))
		for k, item := range value {
			out[k] = deepCopy(item)
		}
		return out
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = deepCopy(item)
		}
		return out
	default:
		return v
	}
}
