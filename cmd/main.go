package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/truenas/truenas-csi/pkg/driver"
	"k8s.io/klog/v2/textlogger"
)

var (
	// CSI protocol flags
	endpoint = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	nodeID   = flag.String("node-id", "", "Node ID")
	mode     = flag.String("mode", "all", "Driver mode: controller, node, or all")
)

func main() {
	logConfig := textlogger.NewConfig()
	logConfig.AddFlags(flag.CommandLine)
	flag.Parse()

	logger := textlogger.NewLogger(logConfig)

	logger.V(driver.LogLevelInfo).Info("Starting TrueNAS CSI Driver", "version", driver.DRIVER_VERSION)

	if err := validateFlags(); err != nil {
		logger.Error(err, "Invalid configuration")
		os.Exit(1)
	}

	config := &driver.DriverConfig{
		NodeID:   *nodeID,
		Endpoint: *endpoint,
		Mode:     driver.DriverMode(*mode),
		Logger:   logger,
	}

	if err := loadEnvConfig(config); err != nil {
		logger.Error(err, "Invalid configuration")
		os.Exit(1)
	}

	d, err := driver.NewDriver(config)
	if err != nil {
		logger.Error(err, "Failed to create driver")
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- d.Run(context.Background())
	}()

	select {
	case err := <-errCh:
		if err != nil {
			logger.Error(err, "Driver failed")
			os.Exit(1)
		}
	case sig := <-sigCh:
		logger.Info("Received signal, shutting down", "signal", sig)
		d.Stop()
	}

	logger.Info("TrueNAS CSI Driver stopped")
}

func validateFlags() error {
	if *nodeID == "" {
		if hostname, err := os.Hostname(); err == nil {
			*nodeID = hostname
		} else {
			return fmt.Errorf("Node ID is required but could not be determined")
		}
	}

	if *endpoint == "" {
		return fmt.Errorf("--endpoint is required")
	}

	// Validate mode
	switch driver.DriverMode(*mode) {
	case driver.DriverModeController, driver.DriverModeNode, driver.DriverModeAll:
		// valid
	default:
		return fmt.Errorf("--mode must be one of: controller, node, all")
	}

	return nil
}

func loadEnvConfig(config *driver.DriverConfig) error {
	// Optional: override the CSI driver name (default: csi.truenas.io).
	// Allows running multiple truenas-csi instances in a single cluster,
	// each pointing at a different TrueNAS appliance, by giving each a
	// unique CSIDriver name (e.g. "tenant-a.csi.truenas.io"). The kubelet
	// socket path and CSIDriver CR name in the deployment manifests must
	// match the value set here.
	config.DriverName = os.Getenv("CSI_DRIVER_NAME")

	if val := os.Getenv("TRUENAS_URL"); val == "" {
		return fmt.Errorf("TRUENAS_URL is missing")
	} else {
		config.TrueNASURL = val
	}

	if val := os.Getenv("TRUENAS_API_KEY"); val == "" {
		return fmt.Errorf("TRUENAS_API_KEY is missing")
	} else {
		config.TrueNASAPIKey = val
	}

	if val := os.Getenv("TRUENAS_DEFAULT_POOL"); val == "" {
		return fmt.Errorf("TRUENAS_DEFAULT_POOL is missing")
	} else {
		config.DefaultPool = val
	}

	// Optional: NFS server and iSCSI portal are derived from TrueNAS URL if not set
	if val := os.Getenv("TRUENAS_NFS_SERVER"); val != "" {
		config.NFSServer = val
	}

	if val := os.Getenv("TRUENAS_ISCSI_PORTAL"); val != "" {
		config.ISCSIPortal = val
	}

	if val := os.Getenv("TRUENAS_ISCSI_IQN_BASE"); val != "" {
		config.ISCSIIQNBase = val
	}

	if val := os.Getenv("TRUENAS_INSECURE_SKIP_VERIFY"); val != "" {
		if insecure, err := strconv.ParseBool(val); err == nil {
			config.TrueNASInsecure = insecure
		}
	}

	return nil
}
