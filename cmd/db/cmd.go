package db

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"

	parseType "github.com/forbole/juno/v4/cmd/parse/types"

	"github.com/forbole/bdjuno/v3/database"
	"github.com/forbole/juno/v4/types/config"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

const (
	FlagsSchemaDir     = "schema-dir"
	MigrationTableName = "schema_migrations"
	MigrationJuno      = "schema_juno"
)

func InitDBCmd(cfg *parseType.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "db",
		PersistentPreRunE: runPersistentPreRuns(parseType.ReadConfigPreRunE(cfg)),
		Long:              "By running this command the juno will load and run schema file from directory, To ensure the juno work properly",
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := os.Stat(config.HomePath); os.IsNotExist(err) {
				err = os.MkdirAll(config.HomePath, os.ModePerm)
				if err != nil {
					return fmt.Errorf("ensuring home dir fail %v", err)
				}
			}

			context, err := parseType.GetParserContext(config.Cfg, cfg)
			if err != nil {
				return fmt.Errorf("build db connection fail %v", err)
			}

			schemaPath, err := cmd.Flags().GetString(FlagsSchemaDir)
			if err != nil {
				return fmt.Errorf("loading schema dir from flag fail %v", err)
			}
			db := database.Cast(context.Database)
			defer db.Close()

			postInit := NewDatabasePostRun(db)
			err = postInit.ProcessSchemaFiles(schemaPath)
			if err != nil {
				return fmt.Errorf("fail to init database schema %v", err)
			}
			log.Info().Msg("init db juno schema successfully")

			if err != nil {
				return err
			}
			if err != nil && !strings.Contains(err.Error(), "no change") {
				log.Err(err).Msg("init shareledger schema got error")
				return err
			}
			return nil
		},
	}
	cmd.Flags().String(FlagsSchemaDir, "", "overrides any existing configuration")

	return cmd
}

func runPersistentPreRuns(preRun func(_ *cobra.Command, _ []string) error) func(_ *cobra.Command, _ []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if root := cmd.Root(); root != nil {
			if root.PersistentPreRunE != nil {
				err := root.PersistentPreRunE(root, args)
				if err != nil {
					return err
				}
			}
		}

		return preRun(cmd, args)
	}
}
