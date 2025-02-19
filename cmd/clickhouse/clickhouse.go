package clickhouse

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
)

var clickHouseShortDescription = "ClickHouse backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var ClickHouseCmd = &cobra.Command{
	Use:     "clickhouse",
	Short:   clickHouseShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "ClickHouse"}, "\t"),
}

func Execute() {
	if err := ClickHouseCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	ClickHouseCmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	ClickHouseCmd.InitDefaultVersionFlag()
}
