package hbase

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	hhttp "github.com/huangnauh/gommon/http"
	"github.com/sirupsen/logrus"
)

type Cell struct {
	Column    string `json:"column"`
	Timestamp int64  `json:"timestamp"`
	Value     string `json:"$"`
}

type Row struct {
	Key   string `json:"key"`
	Cells []Cell `json:"Cell"`
}

type Rows struct {
	Rows []Row `json:"Row"`
}

const (
	defaultRestScanNum   = 10
	defaultRestScanLimit = 1000
	maxScanNum           = 10000
)

var (
	NotFound = errors.New("not found")
)

func getPreviousKey(key string) string {
	if key == "" {
		return ""
	}
	l := len(key)
	c := int(key[l-1]) - 1
	return key[:l-1] + string(c) + "\xff"
}

func getNextKey(key string) string {
	return key + "\x00"
}

func formatRestUrl(addr, table, key string) string {
	return fmt.Sprintf("http://%s/%s/%s", addr, table, url.QueryEscape(key))
}

func hbaseRestScan(addr, table, startRow, endRow string, limit int,
	reversed bool) (*Rows, error) {
	if limit <= 0 {
		limit = defaultRestScanNum
	} else if limit > maxScanNum {
		return nil, fmt.Errorf("limit too large")
	}

	// if startrow or endrow is empty, just let it remain unchanged.
	param := fmt.Sprintf("startrow=%s&endrow=%s&limit=%d",
		url.QueryEscape(startRow), url.QueryEscape(endRow), limit)
	if reversed {
		param = fmt.Sprintf("%s&reversed=true", param)
	}

	url := fmt.Sprintf("http://%s/%s/*?%s", addr, table, param)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	resp, err := hhttp.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		_, _ = io.Copy(ioutil.Discard, resp.Body)
		return nil, fmt.Errorf("scan %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	v := &Rows{}
	err = json.Unmarshal(data, v)
	if err != nil {
		return nil, err
	}
	logrus.Infof("startrow %s, endrow %s, limit %d, reversed %t, found rows %d\n",
		startRow, endRow, limit, reversed, len(v.Rows))
	return v, nil
}

func HbaseRestGetRow(addr, table, row string) ([]byte, error) {
	url := formatRestUrl(addr, table, row)
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	resp, err := hhttp.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		io.Copy(ioutil.Discard, resp.Body)
		if resp.StatusCode == 404 {
			return nil, NotFound
		}
		return nil, fmt.Errorf("get %d", resp.StatusCode)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func StartRestScanner(hbaseAddr, table, startRow, endRow string, limit int,
	timeout time.Duration, input chan<- Row) {
	defer close(input)
	reversed := endRow != "" && strings.Compare(startRow, endRow) > 0
	for {
		if limit == 0 {
			return
		}

		scanLimit := defaultRestScanLimit
		if limit > 0 && limit < scanLimit {
			scanLimit = limit
		}
		scanRes, err := hbaseRestScan(hbaseAddr, table, startRow, endRow, scanLimit, reversed)
		if err != nil {
			logrus.Errorf("scan error, %s\n", err.Error())
			time.Sleep(timeout)
			continue
		}

		for _, v := range scanRes.Rows {
			if limit > 0 {
				limit--
			}
			input <- v
			if limit == 0 {
				break
			}
		}

		// no more rows.
		if len(scanRes.Rows) < scanLimit {
			return
		}

		lastKey, err := base64.StdEncoding.DecodeString(scanRes.Rows[len(scanRes.Rows)-1].Key)
		if err != nil {
			log.Printf("[ERR] decode key error, %s\n", err.Error())
			return
		}

		// avoid repeated scanning for the one row.
		if reversed {
			startRow = getPreviousKey(string(lastKey))
		} else {
			startRow = getNextKey(string(lastKey))
		}
	}
}
