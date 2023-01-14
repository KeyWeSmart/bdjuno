package db

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/forbole/bdjuno/v3/database"
)

type (
	DatabasePostRun struct {
		db *database.Db
	}
	Version struct {
		VersionNum int
		FileName   string
	}
	VersionFile []Version
)

func (v VersionFile) Len() int           { return len(v) }
func (v VersionFile) Less(i, j int) bool { return v[i].VersionNum < v[j].VersionNum }
func (v VersionFile) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }

func NewDatabasePostRun(db *database.Db) DatabasePostRun {
	return DatabasePostRun{db: db}
}

func (d DatabasePostRun) ProcessSchemaFiles(dir string) error {
	err := d.CreateMigrationsTableIfNotExist()
	if err != nil {
		return err
	}
	prevNumber, err := d.GetLastSQLSchemaNumber()
	if err != nil {
		return err
	}
	// 2. load schema file list
	schemaFiles, err := LoadingSchemaFiles(dir)
	if err != nil {
		return fmt.Errorf("get list schema file fail %v", err)
	}

	// 3. get current version if any

	tx, err := d.db.SQL.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	isUpdated := false
	var latestVersion int
	var startFileNumber int
	if prevNumber == nil {
		startFileNumber = -1
	} else {
		startFileNumber = *prevNumber
	}
	for i := range schemaFiles {
		if schemaFiles[i].VersionNum > startFileNumber {
			err = d.ProcessingSQLFile(tx, filepath.Join(dir, schemaFiles[i].FileName))
			if err != nil {
				log.Error().Str("file", schemaFiles[i].FileName).Str("error", err.Error()).Msg("process the schema file fail")
				return fmt.Errorf("fail to process the schema file cause %s", err)
			}
			latestVersion = schemaFiles[i].VersionNum
			isUpdated = true
		}

	}
	if isUpdated {
		log.Info().Msg("updating newest version into db")
		err = d.UpdateLatestVersionNumber(tx, latestVersion)
		if err != nil {
			return fmt.Errorf("updating the new version %v of schema file fail %v", latestVersion, err)
		}
	}
	return tx.Commit()

}

func (d DatabasePostRun) ProcessingSQLFile(tx *sql.Tx, fileName string) error {
	file, err := ioutil.ReadFile(fileName)
	if err != nil {
		return fmt.Errorf("fail to reading the schema file %s", err)
	}
	for _, q := range strings.Split(string(file), ";") {
		if q == "" {
			continue
		}
		log.Debug().Str("file", fileName).Msg("executing query")
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("fail to exec the schema %s query \n \t %s", err, q)
		}
	}
	return nil
}

func LoadingSchemaFiles(dir string) (VersionFile, error) {
	var ver []Version
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, f := range files {
		if !f.IsDir() {
			v, err := parsingFileName(f.Name())
			if err != nil {
				return ver, err
			}
			ver = append(ver, Version{
				VersionNum: v,
				FileName:   f.Name(),
			})
		}
	}
	return ver, nil
}

func (d DatabasePostRun) CreateMigrationsTableIfNotExist() error {
	tx, err := d.db.Sqlx.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s("+
		"latest_number integer,"+
		"one_row_id BOOLEAN NOT NULL DEFAULT TRUE PRIMARY KEY, "+
		"check (one_row_id))", MigrationJuno))

	if err != nil {
		return err
	}
	return tx.Commit()
}

func parsingFileName(fileName string) (version int, err error) {
	versionPrefix := fileName[:strings.Index(fileName, "-")]

	if strings.TrimSpace(versionPrefix) == "" {
		return 0, fmt.Errorf("parsing the version from file name fail")
	}

	v, err := strconv.ParseInt(versionPrefix, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("version number is invalid")
	}
	return int(v), nil

}

func (d DatabasePostRun) UpdateLatestVersionNumber(tx *sql.Tx, v int) error {

	_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (latest_number) VALUES ($1) ON CONFLICT (one_row_id) DO UPDATE SET latest_number=$2", MigrationJuno), v, v)

	if err != nil {
		return err
	}
	return nil
}

func (d DatabasePostRun) GetLastSQLSchemaNumber() (latestNumber *int, err error) {
	r := d.db.Sqlx.QueryRow(fmt.Sprintf("SELECT latest_number FROM %s ORDER BY latest_number DESC LIMIT 1", MigrationJuno))
	if err := r.Scan(&latestNumber); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return latestNumber, nil
}
