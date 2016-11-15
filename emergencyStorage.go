package emergancyStorage

import (
	"bufio"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileStorageConfig struct {
	FilePath           string `json:"filePath" bson:"filePath"`
	FileNamePrefix     string `json:"fileNamePrefix" bson:"fileNamePrefix"`
	ChannelBufferLimit int64  `json:"channelBufferLimit" bson:"channelBufferLimit"`
}

type FileStorageObjectBuilder interface {
	New() interface {
		BuildByString(str string) (FileStorageItem, error)
	}
}

type FileStorageItem interface {
	MarshalBinary() ([]byte, error)
	BinaryUnmarshaler(data []byte) error
	GetStringIdent() string
}

type FileStorage struct {
	cfg          FileStorageConfig
	writeChannel chan FileStorageItem
	currentFile  *os.File
	objBuilder   interface {
		BuildByString(str string) (FileStorageItem, error)
	}
	mxt sync.Mutex
}

func NewFileStorage(cfg FileStorageConfig, objBuilder FileStorageObjectBuilder) *FileStorage {
	result := FileStorage{}
	result.cfg = cfg
	result.writeChannel = make(chan FileStorageItem, result.cfg.ChannelBufferLimit)
	result.objBuilder = objBuilder.New()

	file, err := os.OpenFile(result.getCurrentFileName(), os.O_CREATE|os.O_RDWR, 0666)
	result.currentFile = nil
	if err != nil {
		log.Errorf("Error open filestorage: %s", err.Error())

	}
	file.Close()

	go func(itChan chan FileStorageItem) {
		for item := range itChan {
			result.writeToFile(item)
		}

	}(result.writeChannel)
	return &result
}

func (f *FileStorage) WriteItem(item FileStorageItem) {
	f.writeChannel <- item
}

func (f *FileStorage) ReadToChannel(readChannel chan FileStorageItem) {
	go func() {
		time.Sleep(time.Second * 1)
		files, err := ioutil.ReadDir(f.cfg.FilePath)
		if err != nil {
			log.Errorf("Read dir error: %v\n", err)
		}
		for _, file := range files {
			if _, err := os.Stat(f.cfg.FilePath + file.Name()); err != nil && file.IsDir() {
				continue
			}

			f.mxt.Lock()
			match, _ := filepath.Match(f.cfg.FileNamePrefix+"*.pwlds", file.Name())

			if !match {
				f.mxt.Unlock()
				continue
			}
			if f.currentFile != nil {
				fInfo, err := f.currentFile.Stat()
				if err == nil && fInfo.Name() != f.getFileName() {
					f.currentFile.Close()
					f.currentFile = nil
				} else {
					f.mxt.Unlock()
					continue
				}
			}
			f.mxt.Unlock()
			curFile, err := os.OpenFile(f.cfg.FilePath+file.Name(), os.O_RDWR, 0666)
			if err == nil {
				lines, err := f.readFileLines(curFile)
				if err == nil {
					for _, line := range lines {
						if line != "" {
							if err == nil {
								item, err := f.objBuilder.BuildByString(line)
								if err != nil {
									log.Errorf("Error build FileSorageItem: %s, string %s", err.Error(), line)
								}
								readChannel <- item
							}
						} else {
							log.Errorf("Error read FileSorageItem: %s, string %s", err.Error(), line)
						}
					}
				}
				curFile.Close()
				err = os.Remove(f.cfg.FilePath + file.Name())
			}

		}

	}()
}

func (f *FileStorage) getCurrentFileName() string {
	return f.cfg.FilePath + f.cfg.FileNamePrefix + f.getFileName()
}

func (f *FileStorage) getFileName() string {
	return time.Now().Format("_2006-01-02_15:04:05") + ".pwlds"
}

func (f *FileStorage) writeToFile(item FileStorageItem) error {
	f.mxt.Lock()
	if f.currentFile != nil {
		fInfo, err := f.currentFile.Stat()
		if err != nil || fInfo.Name() != f.getCurrentFileName() {
			f.currentFile.Close()
			f.currentFile, err = os.OpenFile(f.getCurrentFileName(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		}
	} else {
		f.currentFile, _ = os.OpenFile(f.getCurrentFileName(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	}

	lineBytes, err := item.MarshalBinary()
	if err != nil {
		log.Errorf("Error marshaling %s  data %#v", err.Error(), item)
	}
	_, err = f.currentFile.WriteString(string(lineBytes) + "\n")

	if err != nil {
		log.Errorf("Error write to local storage %s  data %s", err.Error(), string(lineBytes))
	}
	f.mxt.Unlock()

	return err
}

func (f *FileStorage) readFileLines(file *os.File) ([]string, error) {
	var result []string
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	return result, scanner.Err()
}
