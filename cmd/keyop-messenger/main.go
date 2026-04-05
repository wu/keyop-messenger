// Command keyop-messenger provides the CLI for keyop-messenger key management.
package main

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/wu/keyop-messenger/internal/tlsutil"
	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "keyop-messenger",
		Short:        "keyop-messenger — federated pub-sub daemon and key management",
		SilenceUsage: true,
	}
	root.AddCommand(versionCmd())
	root.AddCommand(keygenCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version",
		RunE: func(cmd *cobra.Command, args []string) error {
			v := "(devel)"
			if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
				v = info.Main.Version
			}
			fmt.Fprintf(cmd.OutOrStdout(), "version: %s\n", v)
			return nil
		},
	}
}

func keygenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keygen",
		Short: "Generate TLS certificates for federation",
	}
	cmd.AddCommand(keygenCACmd())
	cmd.AddCommand(keygenInstanceCmd())
	return cmd
}

func keygenCACmd() *cobra.Command {
	var (
		outCert string
		outKey  string
		days    int
		force   bool
	)
	cmd := &cobra.Command{
		Use:   "ca",
		Short: "Generate a self-signed CA certificate and private key",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkOverwrite(force, outCert, outKey); err != nil {
				return err
			}
			certPEM, keyPEM, err := tlsutil.GenerateCA(days)
			if err != nil {
				return fmt.Errorf("generate CA: %w", err)
			}
			if err := writePEM(outCert, certPEM); err != nil {
				return err
			}
			if err := writePEM(outKey, keyPEM); err != nil {
				return err
			}
			printCertInfo(cmd, certPEM)
			return nil
		},
	}
	cmd.Flags().StringVar(&outCert, "out-cert", "ca.crt", "output certificate file")
	cmd.Flags().StringVar(&outKey, "out-key", "ca.key", "output private key file")
	cmd.Flags().IntVar(&days, "days", 730, "validity period in days")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}

func keygenInstanceCmd() *cobra.Command {
	var (
		caFile  string
		caKey   string
		name    string
		outCert string
		outKey  string
		days    int
		force   bool
	)
	cmd := &cobra.Command{
		Use:   "instance",
		Short: "Generate an instance certificate signed by a CA",
		RunE: func(cmd *cobra.Command, args []string) error {
			if outCert == "" {
				outCert = name + ".crt"
			}
			if outKey == "" {
				outKey = name + ".key"
			}
			if err := checkOverwrite(force, outCert, outKey); err != nil {
				return err
			}
			caCertPEM, err := os.ReadFile(caFile)
			if err != nil {
				return fmt.Errorf("read CA cert %q: %w", caFile, err)
			}
			caKeyPEM, err := os.ReadFile(caKey)
			if err != nil {
				return fmt.Errorf("read CA key %q: %w", caKey, err)
			}
			certPEM, keyPEM, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, name, days)
			if err != nil {
				return fmt.Errorf("generate instance cert: %w", err)
			}
			if err := writePEM(outCert, certPEM); err != nil {
				return err
			}
			if err := writePEM(outKey, keyPEM); err != nil {
				return err
			}
			printCertInfo(cmd, certPEM)
			return nil
		},
	}
	cmd.Flags().StringVar(&caFile, "ca", "", "CA certificate file (required)")
	cmd.Flags().StringVar(&caKey, "ca-key", "", "CA private key file (required)")
	cmd.Flags().StringVar(&name, "name", "", "instance name used as CN and DNS SAN (required)")
	cmd.Flags().StringVar(&outCert, "out-cert", "", "output certificate file (default: <name>.crt)")
	cmd.Flags().StringVar(&outKey, "out-key", "", "output private key file (default: <name>.key)")
	cmd.Flags().IntVar(&days, "days", 730, "validity period in days")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	_ = cmd.MarkFlagRequired("ca")
	_ = cmd.MarkFlagRequired("ca-key")
	_ = cmd.MarkFlagRequired("name")
	return cmd
}

// checkOverwrite returns an error if any file already exists and force is false.
func checkOverwrite(force bool, files ...string) error {
	if force {
		return nil
	}
	for _, f := range files {
		if _, err := os.Stat(f); err == nil {
			return fmt.Errorf("file %q already exists; use --force to overwrite", f)
		}
	}
	return nil
}

// writePEM writes data to path with mode 0600.
func writePEM(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

// printCertInfo writes the subject, validity period, and SHA-256 fingerprint of
// the first certificate in certPEM to cmd's output writer.
func printCertInfo(cmd *cobra.Command, certPEM []byte) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return
	}
	sum := sha256.Sum256(cert.Raw)
	fmt.Fprintf(cmd.OutOrStdout(), "Subject:     %s\n", cert.Subject)
	fmt.Fprintf(cmd.OutOrStdout(), "Valid from:  %s\n", cert.NotBefore.Format(time.RFC3339))
	fmt.Fprintf(cmd.OutOrStdout(), "Valid until: %s\n", cert.NotAfter.Format(time.RFC3339))
	fmt.Fprintf(cmd.OutOrStdout(), "Fingerprint: SHA-256:%s\n", hex.EncodeToString(sum[:]))
}
