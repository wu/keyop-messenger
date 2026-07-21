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
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wu/keyop-messenger/internal/tlsutil"
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			v := "(devel)"
			if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
				v = info.Main.Version
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "version: %s\n", v)
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
	cmd.AddCommand(keygenInspectCmd())
	return cmd
}

func keygenInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <cert-file>",
		Short: "Show CN, validity period, and other details of an existing certificate",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			certPEM, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read cert file: %w", err)
			}
			block, _ := pem.Decode(certPEM)
			if block == nil {
				return fmt.Errorf("no PEM block found in %s", args[0])
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return fmt.Errorf("parse cert: %w", err)
			}
			printDetailedCertInfo(cmd, cert)
			return nil
		},
	}
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
		RunE: func(cmd *cobra.Command, _ []string) error {
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			if outCert == "" {
				outCert = name + ".crt"
			}
			if outKey == "" {
				outKey = name + ".key"
			}
			if err := checkOverwrite(force, outCert, outKey); err != nil {
				return err
			}
			caCertPEM, err := os.ReadFile(caFile) // #nosec G304 -- CLI flag, trusted operator input
			if err != nil {
				return fmt.Errorf("read CA cert %q: %w", caFile, err)
			}
			caKeyPEM, err := os.ReadFile(caKey) // #nosec G304 -- CLI flag, trusted operator input
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
	if err := os.WriteFile(path, data, 0o600); err != nil { // #nosec G703 -- CLI flag, trusted operator input
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

// printCertInfo writes the subject, validity period, and SHA-256 fingerprint of
// the first certificate in certPEM to cmd's output writer. Used by the
// keygen ca / keygen instance commands as a confirmation of what was issued.
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
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Subject:     %s\n", cert.Subject)
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Valid from:  %s\n", cert.NotBefore.Format(time.RFC3339))
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Valid until: %s\n", cert.NotAfter.Format(time.RFC3339))
	_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Fingerprint: SHA-256:%s\n", hex.EncodeToString(sum[:]))
}

// printDetailedCertInfo writes a richer summary of a parsed certificate, used
// by the keygen inspect command. Surfaces the CN explicitly (the field most
// load-bearing for federation identity) and includes a relative
// expiry-countdown so an operator can spot a soon-to-expire cert at a glance.
func printDetailedCertInfo(cmd *cobra.Command, cert *x509.Certificate) {
	w := cmd.OutOrStdout()
	sum := sha256.Sum256(cert.Raw)

	_, _ = fmt.Fprintf(w, "Subject CN:    %s\n", cert.Subject.CommonName)
	_, _ = fmt.Fprintf(w, "Subject:       %s\n", cert.Subject)
	_, _ = fmt.Fprintf(w, "Issuer CN:     %s\n", cert.Issuer.CommonName)
	if len(cert.DNSNames) > 0 {
		_, _ = fmt.Fprintf(w, "DNS SANs:      %s\n", strings.Join(cert.DNSNames, ", "))
	}
	if len(cert.IPAddresses) > 0 {
		ips := make([]string, len(cert.IPAddresses))
		for i, ip := range cert.IPAddresses {
			ips[i] = ip.String()
		}
		_, _ = fmt.Fprintf(w, "IP SANs:       %s\n", strings.Join(ips, ", "))
	}
	_, _ = fmt.Fprintf(w, "Valid from:    %s\n", cert.NotBefore.Format(time.RFC3339))
	_, _ = fmt.Fprintf(w, "Valid until:   %s  (%s)\n",
		cert.NotAfter.Format(time.RFC3339), describeExpiry(cert.NotAfter))
	_, _ = fmt.Fprintf(w, "Is CA:         %v\n", cert.IsCA)
	if len(cert.ExtKeyUsage) > 0 {
		labels := make([]string, 0, len(cert.ExtKeyUsage))
		for _, u := range cert.ExtKeyUsage {
			labels = append(labels, extKeyUsageLabel(u))
		}
		_, _ = fmt.Fprintf(w, "Ext key usage: %s\n", strings.Join(labels, ", "))
	}
	_, _ = fmt.Fprintf(w, "Serial:        %s\n", cert.SerialNumber.String())
	_, _ = fmt.Fprintf(w, "Fingerprint:   SHA-256:%s\n", hex.EncodeToString(sum[:]))
}

// describeExpiry returns a short human-readable "expires in N days" or
// "EXPIRED N days ago" relative to now, suitable for inline display.
func describeExpiry(notAfter time.Time) string {
	d := time.Until(notAfter)
	switch {
	case d < 0:
		days := int(-d.Hours() / 24)
		if days == 0 {
			return fmt.Sprintf("EXPIRED %s ago", (-d).Round(time.Minute))
		}
		return fmt.Sprintf("EXPIRED %d day(s) ago", days)
	case d < 24*time.Hour:
		return fmt.Sprintf("expires in %s", d.Round(time.Minute))
	default:
		return fmt.Sprintf("expires in %d day(s)", int(d.Hours()/24))
	}
}

// extKeyUsageLabel maps an x509.ExtKeyUsage value to a short human-readable
// name. EKUs relevant to a federation cert are listed explicitly; the niche
// values (IPSEC, Microsoft/Netscape proprietary) fall through to the
// default-rendered "eku(N)" form. This is a display helper, not a security
// gate, so the default case is intentional rather than a TODO.
func extKeyUsageLabel(u x509.ExtKeyUsage) string {
	switch u { //nolint:exhaustive // niche EKUs handled by the default case
	case x509.ExtKeyUsageAny:
		return "any"
	case x509.ExtKeyUsageServerAuth:
		return "serverAuth"
	case x509.ExtKeyUsageClientAuth:
		return "clientAuth"
	case x509.ExtKeyUsageCodeSigning:
		return "codeSigning"
	case x509.ExtKeyUsageEmailProtection:
		return "emailProtection"
	case x509.ExtKeyUsageTimeStamping:
		return "timeStamping"
	case x509.ExtKeyUsageOCSPSigning:
		return "ocspSigning"
	default:
		return fmt.Sprintf("eku(%d)", u)
	}
}
