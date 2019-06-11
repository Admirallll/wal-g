package clickhouse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	_ "github.com/kshvakov/clickhouse"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	shadowSuffix = "shadow"
	metaSuffix   = "metadata"
)

func HandleBackupPush(uploader *internal.Uploader) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	timeStart := utility.TimeNowCrossPlatformLocal()

	backupName := utility.TimeNowCrossPlatformUTC().Format("20060102T150405Z")

	ch := ClickHouse{}
	clickHouseConfig := ClickHouseConfig{host: "localhost", password: "1234", port: 9000, login: "default"}
	ch.Connect(clickHouseConfig)
	defer ch.Close()
	clickHouseFolderPath, _ := ch.GetDataPath()
	err := os.RemoveAll(path.Join(clickHouseFolderPath, "shadow"))
	if err != nil {
		fmt.Println("error on remove files", err)
	}
	tables, _ := ch.GetTables()

	err = ch.Freeze(tables)
	if err != nil {
		fmt.Println(err)
	}

	err = uploader.UploadDirectory(path.Join(clickHouseFolderPath, shadowSuffix), path.Join(backupName, shadowSuffix))
	if err != nil {
		fmt.Println(err)
	}
	err = uploader.UploadDirectory(path.Join(clickHouseFolderPath, metaSuffix), path.Join(backupName, metaSuffix))
	if err != nil {
		fmt.Println(err)
	}
	err = uploadStreamSentinel(&StreamSentinelDto{StartLocalTime: timeStart}, uploader, backupName+utility.SentinelSuffix)
	if err != nil {
		fmt.Println(err)
	}
}

type StreamSentinelDto struct {
	StartLocalTime time.Time
}

func uploadStreamSentinel(sentinelDto *StreamSentinelDto, uploader *internal.Uploader, name string) error {
	dtoBody, err := json.Marshal(*sentinelDto)
	if err != nil {
		return err
	}

	uploadingErr := uploader.Upload(name, bytes.NewReader(dtoBody))
	if uploadingErr != nil {
		tracelog.ErrorLogger.Printf("upload: could not upload '%s'\n", name)
		tracelog.ErrorLogger.Fatalf("StorageTarBall finish: json failed to upload")
		return uploadingErr
	}
	return nil
}
