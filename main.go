package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"os"
)

type Catalog struct {
	db   *sql.DB
	path string
}

func NewCatalog(path string) (*Catalog, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &Catalog{
		db:   db,
		path: path,
	}, nil
}

const (
	select_columns = `
select
      root.absolutePath
    , folder.pathFromRoot
    , file.baseName
    , file.extension
    , file.sidecarExtensions
`

	select_count = `select count(*) `

	select_where = `
from        AgLibraryFile           as file

inner join  Adobe_images            as image
on          file.id_local = image.rootFile

inner join  AgLibraryFolder         as folder
on          file.folder = folder.id_local

inner join  AgLibraryRootFolder     as root
on          folder.rootFolder = root.id_local

where       file.sidecarExtensions  = 'JPG'
and         image.fileFormat        = 'RAW'
`
)

func (c *Catalog) GetSidecarCount() (int, error) {
	row := c.db.QueryRow(select_count + select_where)
	count := -1
	err := row.Scan(&count)
	return count, err
}

func (c *Catalog) getSidecarRows() (*sql.Rows, error) {
	return c.db.Query(select_columns + select_where)
}

func (c *Catalog) ProcessSidecars(handler func(string, string) error) error {
	rows, err := c.getSidecarRows()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var rootPath string
		var filePath string
		var fileName string
		var extension string
		var sidecarExtension string

		err = rows.Scan(&rootPath, &filePath, &fileName, &extension, &sidecarExtension)
		if err != nil {
			return err
		}
		sidecarPath := fmt.Sprintf("%s%s%s.%s", rootPath, filePath, fileName, sidecarExtension)
		originalPath := fmt.Sprintf("%s%s%s.%s", rootPath, filePath, fileName, extension)

		handler(sidecarPath, originalPath)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Must supply a path to a Lightroom catalog file.")
	}
	path := os.Args[1]
	log.Printf("Processing %s", path)

	catalog, err := NewCatalog(path)
	if err != nil {
		log.Fatal(err)
	}

	count, err := catalog.GetSidecarCount()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("There are %d sidecar entries.", count)

	var processed_count uint
	var error_count uint
	var skip_count uint
	var missing_count uint
	catalog.ProcessSidecars(func(sidecarPath string, originalPath string) error {
		if _, err := os.Stat(sidecarPath); err == nil {
			if _, err := os.Stat(originalPath); os.IsNotExist(err) {
				log.Printf("Missing original path for sidecar; Skipping %s", sidecarPath)
				skip_count++
				return nil
			}

			log.Printf("Deleting %s", sidecarPath)
			err = os.Remove(sidecarPath)
			if err != nil {
				log.Printf("Error deleting %s; %v.", sidecarPath, err)
				error_count++
				return err
			} else {
				processed_count++
			}
		} else {
			log.Printf("Missing %s", sidecarPath)
			missing_count++
		}
		return nil
	})
	log.Printf("Done.")
	log.Printf("   Total:   %d", count)
	log.Printf("   Deleted: %d", processed_count)
	log.Printf("   Skipped: %d", skip_count)
	log.Printf("   Missing: %d", missing_count)
}
