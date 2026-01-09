package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/iXsystems/truenas_k8_driver/pkg/driver"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
)

var (
	// CSI protocol flags
	endpoint = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
	nodeID   = flag.String("node-id", "", "Node ID")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	klog.V(driver.LogLevelInfo).InfoS("Starting TrueNAS CSI Driver", "version", driver.DRIVER_VERSION)

	if err := validateFlags(); err != nil {
		klog.ErrorS(err, "Invalid configuration")
		os.Exit(1)
	}

	config := &driver.DriverConfig{
		NodeID:   *nodeID,
		Endpoint: *endpoint,
		Logger:   textlogger.NewLogger(textlogger.NewConfig()),
	}

	if err := loadEnvConfig(config); err != nil {
		klog.ErrorS(err, "Invalid configuration")
		os.Exit(1)
	}

	d, err := driver.NewDriver(config)
	if err != nil {
		klog.ErrorS(err, "Failed to create driver")
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
			klog.ErrorS(err, "Driver failed")
			os.Exit(1)
		}
	case sig := <-sigCh:
		klog.InfoS("Received signal, shutting down", "signal", sig)
		d.Stop()
	}

	klog.InfoS("TrueNAS CSI Driver stopped")
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

	return nil
}

func loadEnvConfig(config *driver.DriverConfig) error {
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
