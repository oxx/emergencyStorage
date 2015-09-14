package emergancyStorage

import (
	"testing"
	"strings"
	"encoding/json"
	"fmt"
	"strconv"
	"reflect"
	"time"

)


type TestObj struct {
	Name   string `json:"name"`
	String string `json:"string"`
}

type TestBuilder struct {

}

type TestBuilderFromString struct {

}

type TestError struct {
	Str string
}


func (e TestError) Error() string {
	return fmt.Sprint(e.Str)
}



func (p TestObj) MarshalBinary() ([]byte, error) {
	data, err := json.Marshal(p)
	return data, err

}

func (p TestObj) BinaryUnmarshaler(data []byte) error {
	err := json.Unmarshal(data, p)
	return err
}

func (p TestObj) GetStringIdent() string {
	return "\"name\":\"testObject\""
}


func NewTestObject(str string) TestObj {
	return TestObj{"testObject", str}
}


func (e TestBuilder) New() interface{ buildByString(str string) (FileStorageItem, error)} {
	return &TestBuilderFromString{}
}

func (eb *TestBuilderFromString) buildByString(str string) (FileStorageItem, error) {
	if strings.Contains(str, TestObj{}.GetStringIdent()) {
		var obj = TestObj{}
		err := json.Unmarshal([]byte(str), &obj)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}
	return nil, TestError{fmt.Sprintf("Wrong string for build object: %s", str)}
}

func TestStorage(t *testing.T) {
	conf := FileStorageConfig{"/tmp/", "estest", 10}
	builder := TestBuilder{}
	storage := NewFileStorage(conf, builder)
	var testSlice []FileStorageItem
	for i := 1; i <= 10; i++ {
		obj := NewTestObject(strconv.Itoa(i))
		storage.WriteItem(obj)
		testSlice = append(testSlice, obj)
	}

	readChannel := make(chan FileStorageItem, 10)
	var rSlice  []FileStorageItem
	timer := time.NewTimer(time.Second).C
	storage.ReadToChannel(readChannel)
	for {
		select {
		case rObj := <-readChannel:
			rSlice = append(rSlice, rObj)
		case <-timer:
			if !reflect.DeepEqual(testSlice, rSlice) {
				t.Errorf("Expected equeal slices")
			}
			return
		}
	}



}