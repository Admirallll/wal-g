package internal

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// Uploader contains fields associated with uploading tarballs.
// Multiple tarballs can share one uploader.
type Uploader struct {
	UploadingFolder  storage.Folder
	Compressor       compression.Compressor
	waitGroup        *sync.WaitGroup
	deltaFileManager *DeltaFileManager
	Failed           atomic.Value
}

func (uploader *Uploader) getUseWalDelta() (useWalDelta bool) {
	return uploader.deltaFileManager != nil
}

func NewUploader(
	compressor compression.Compressor,
	uploadingLocation storage.Folder,
	deltaFileManager *DeltaFileManager,
) *Uploader {
	uploader := &Uploader{
		UploadingFolder:  uploadingLocation,
		Compressor:       compressor,
		waitGroup:        &sync.WaitGroup{},
		deltaFileManager: deltaFileManager,
	}
	uploader.Failed.Store(false)
	return uploader
}

// finish waits for all waiting parts to be uploaded. If an error occurs,
// prints alert to stderr.
func (uploader *Uploader) finish() {
	uploader.waitGroup.Wait()
	if uploader.Failed.Load().(bool) {
		tracelog.ErrorLogger.Printf("WAL-G could not complete upload.\n")
	}
}

// Clone creates similar Uploader with new WaitGroup
func (uploader *Uploader) Clone() *Uploader {
	return &Uploader{
		uploader.UploadingFolder,
		uploader.Compressor,
		&sync.WaitGroup{},
		uploader.deltaFileManager,
		uploader.Failed,
	}
}

// TODO : unit tests
func (uploader *Uploader) UploadWalFile(file NamedReader) error {
	var walFileReader io.Reader

	filename := path.Base(file.Name())
	if uploader.getUseWalDelta() && isWalFilename(filename) {
		recordingReader, err := NewWalDeltaRecordingReader(file, filename, uploader.deltaFileManager)
		if err != nil {
			walFileReader = file
		} else {
			walFileReader = recordingReader
			defer recordingReader.Close()
		}
	} else {
		walFileReader = file
	}

	return uploader.UploadFile(&NamedReaderImpl{walFileReader, file.Name()})
}

func (uploader *Uploader) UploadDirectory(path string, name string) error {
	tmpFile, err := ioutil.TempFile("", "ch_backup_")
	if err != nil {
		return err
	}
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())
	err = zipDirectory(path, tmpFile)
	if err != nil {
		return fmt.Errorf("Error on zipping directory: %v", err)
	}
	file, err := os.Open(tmpFile.Name())
	if err != nil {
		return err
	}
	namedReader := NewNamedReaderImpl(file, name)
	return uploader.UploadFileWithSanitizedName(namedReader)
}

func zipDirectory(pathToFolder string, tempFile *os.File) error {
	zipWriter := zip.NewWriter(tempFile)
	defer zipWriter.Close()
	return filepath.Walk(pathToFolder, func(s string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return addFileToZip(zipWriter, s)
	})
}

func addFileToZip(zipWriter *zip.Writer, filename string) error {
	fileToZip, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = filename

	// Change to deflate to gain better compression
	// see http://golang.org/pkg/archive/zip/#pkg-constants
	header.Method = zip.Deflate

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}

// TODO : unit tests
// UploadFile compresses a file and uploads it.
func (uploader *Uploader) UploadFile(file NamedReader) error {
	newFile := NewNamedReaderImpl(file, utility.SanitizePath(filepath.Base(file.Name())))

	return uploader.UploadFileWithSanitizedName(newFile)
}

func (uploader *Uploader) UploadFileWithSanitizedName(file NamedReader) error {
	compressedFile := CompressAndEncrypt(file, uploader.Compressor, ConfigureCrypter())
	dstPath := file.Name() + "." + uploader.Compressor.FileExtension()
	err := uploader.Upload(dstPath, compressedFile)
	tracelog.InfoLogger.Println("FILE PATH:", dstPath)
	return err
}

// TODO : unit tests
func (uploader *Uploader) Upload(path string, content io.Reader) error {
	err := uploader.UploadingFolder.PutObject(path, content)
	if err == nil {
		return nil
	}
	uploader.Failed.Store(true)
	tracelog.ErrorLogger.Printf(tracelog.GetErrorFormatter()+"\n", err)
	return err
}
