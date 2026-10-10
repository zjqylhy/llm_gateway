package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func RunRecoveryCommand(c context.Context, r *Runtime, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: metering-recover plan [--output path] | apply --manifest path --hash sha256")
	}
	flags := flag.NewFlagSet("metering-recover "+args[0], flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("manifest", "", "manifest path")
	hash := flags.String("hash", "", "confirmed manifest hash")
	output := flags.String("output", "", "write plan JSON")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected recovery arguments")
	}
	switch args[0] {
	case "plan":
		if *path != "" || *hash != "" {
			return errors.New("plan accepts only --output")
		}
		m, err := PlanMeteringRecovery(c, r)
		if err != nil {
			return err
		}
		b, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		b = append(b, '\n')
		if *output != "" {
			if err = os.WriteFile(*output, b, 0600); err != nil {
				return err
			}
			fmt.Println("manifest_hash:", m.Hash)
			return nil
		}
		_, err = os.Stdout.Write(b)
		return err
	case "apply":
		if *output != "" || *path == "" || *hash == "" {
			return errors.New("apply requires --manifest and --hash")
		}
		m, err := LoadRecoveryManifest(*path)
		if err != nil {
			return err
		}
		if *hash != m.Hash {
			return errors.New("confirmation hash differs from manifest")
		}
		result, err := ApplyMeteringRecovery(c, r, m)
		if err != nil {
			return err
		}
		fmt.Printf("recovery verified: groups=%d attempts=%d pending=%d; metering remains frozen\n", result.Groups, result.Attempts, result.Pending)
		return nil
	default:
		return errors.New("recovery action must be plan or apply")
	}
}
