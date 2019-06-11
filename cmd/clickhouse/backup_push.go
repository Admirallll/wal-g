package clickhouse

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/clickhouse"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const streamPushShortDescription = "Makes backup and uploads it to storage"

var backupPushCmd = &cobra.Command{
	Use:   "backup-push",
	Short: streamPushShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		uploader, err := internal.ConfigureUploader()
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}
		clickhouse.HandleBackupPush(uploader)
	},
}

func init() {
	ClickHouseCmd.AddCommand(backupPushCmd)
}
