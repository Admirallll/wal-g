package clickhouse

import (
	"fmt"
	"path"
	"strings"

	"github.com/jmoiron/sqlx"
)

type ClickHouse struct {
	conn *sqlx.DB
}

type Table struct {
	Database     string `db:"database"`
	Name         string `db:"name"`
	DataPath     string `db:"data_path"`
	MetadataPath string `db:"metadata_path"`
	IsTemporary  bool   `db:"is_temporary"`
	Skip         bool
}

type ClickHouseConfig struct {
	login    string
	password string
	host     string
	port     int
}

func (ch *ClickHouse) Connect(config ClickHouseConfig) error {
	connectionString := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s&compress=true", config.host, config.port, config.login, config.password)
	var err error
	if ch.conn, err = sqlx.Open("clickhouse", connectionString); err != nil {
		return err
	}
	return ch.conn.Ping()
}

func (ch *ClickHouse) Close() error {
	return ch.conn.Close()
}

func (ch *ClickHouse) GetTables() ([]Table, error) {
	var tables []Table
	if err := ch.conn.Select(&tables, "SELECT database, name, is_temporary, data_path, metadata_path FROM system.tables WHERE data_path != '' AND is_temporary = 0;"); err != nil {
		return nil, err
	}
	return tables, nil
}

func (ch *ClickHouse) Freeze(tables []Table) error {
	for _, table := range tables {
		query := "ALTER TABLE `" + table.Name + "` FREEZE PARTITION tuple();"
		_, err := ch.conn.Exec(query)

		if err != nil {
			return err
		}
	}

	return nil
}

func (ch *ClickHouse) GetDataPath() (string, error) {
	var result []struct {
		MetadataPath string `db:"metadata_path"`
	}
	if err := ch.conn.Select(&result, "SELECT metadata_path FROM system.tables WHERE database == 'system' LIMIT 1;"); err != nil {
		return "/var/lib/clickhouse", err
	}
	metadataPath := result[0].MetadataPath
	dataPathArray := strings.Split(metadataPath, "/")
	clickhouseData := path.Join(dataPathArray[:len(dataPathArray)-3]...)
	return path.Join("/", clickhouseData), nil
}
