/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

type Conn struct {
	config config
	mapi   *MapiConn
}

var c driver.Execer = &Conn{}

func newConn(c config) (*Conn, error) {
	conn := &Conn{
		config: c,
		mapi:   nil,
	}

	m := NewMapi(c.Hostname, c.Port, c.Username, c.Password, c.Database, "sql")
	err := m.Connect()
	if err != nil {
		return conn, err
	}

	conn.mapi = m
	return conn, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query), nil
}

func (c *Conn) Close() error {
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	t := newTx(c)

	_, err := c.execute("START TRANSACTION")
	if err != nil {
		t.err = err
	}

	return t, t.err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	// don't support interpolating params in non statement Execs
	if len(args) > 0 {
		return nil, driver.ErrSkip
	}

	res := newResult()
	r, err := c.execute(query)
	if err != nil {
		res.err = err
		return res, res.err
	}

	qr := &queryResults{}
	err = qr.storeResult(r)
	res.lastInsertId = qr.lastRowId
	res.rowsAffected = qr.rowCount
	res.err = err

	return res, res.err
}

func (c *Conn) cmd(cmd string) (string, error) {
	if c.mapi == nil {
		return "", fmt.Errorf("Database connection closed")
	}

	return c.mapi.Cmd(cmd)
}

func (c *Conn) execute(q string) (string, error) {
	cmd := fmt.Sprintf("s%s;", q)
	return c.cmd(cmd)
}

type queryResults struct {
	lastRowId   int
	rowCount    int
	queryId     int
	offset      int
	columnCount int
	execId      int

	rows        [][]driver.Value
	description []description
}

func (qr *queryResults) storeResult(r string) error {
	var columnNames []string
	var columnTypes []string
	var displaySizes []int
	var internalSizes []int
	var precisions []int
	var scales []int
	var nullOks []int

	for _, line := range strings.Split(r, "\n") {
		if strings.HasPrefix(line, mapi_MSG_INFO) {
			// TODO log

		} else if strings.HasPrefix(line, mapi_MSG_QPREPARE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			qr.execId, _ = strconv.Atoi(t[0])
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_QTABLE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			qr.queryId, _ = strconv.Atoi(t[0])
			qr.rowCount, _ = strconv.Atoi(t[1])
			qr.columnCount, _ = strconv.Atoi(t[2])

			columnNames = make([]string, qr.columnCount)
			columnTypes = make([]string, qr.columnCount)
			displaySizes = make([]int, qr.columnCount)
			internalSizes = make([]int, qr.columnCount)
			precisions = make([]int, qr.columnCount)
			scales = make([]int, qr.columnCount)
			nullOks = make([]int, qr.columnCount)

		} else if strings.HasPrefix(line, mapi_MSG_TUPLE) {
			v, err := qr.parseTuple(line)
			if err != nil {
				return err
			}
			qr.rows = append(qr.rows, v)

		} else if strings.HasPrefix(line, mapi_MSG_QBLOCK) {
			qr.rows = make([][]driver.Value, 0)

		} else if strings.HasPrefix(line, mapi_MSG_QSCHEMA) {
			qr.offset = 0
			qr.rows = make([][]driver.Value, 0)
			qr.lastRowId = 0
			qr.description = nil
			qr.rowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_QUPDATE) {
			t := strings.Split(strings.TrimSpace(line[2:]), " ")
			qr.rowCount, _ = strconv.Atoi(t[0])
			qr.lastRowId, _ = strconv.Atoi(t[1])

		} else if strings.HasPrefix(line, mapi_MSG_QTRANS) {
			qr.offset = 0
			qr.rows = make([][]driver.Value, 0, 0)
			qr.lastRowId = 0
			qr.description = nil
			qr.rowCount = 0

		} else if strings.HasPrefix(line, mapi_MSG_HEADER) {
			t := strings.Split(line[1:], "#")
			data := strings.TrimSpace(t[0])
			identity := strings.TrimSpace(t[1])

			values := make([]string, 0)
			for _, value := range strings.Split(data, ",") {
				values = append(values, strings.TrimSpace(value))
			}

			if identity == "name" {
				columnNames = values

			} else if identity == "type" {
				columnTypes = values

			} else if identity == "typesizes" {
				sizes := make([][]int, len(values))
				for i, value := range values {
					s := make([]int, 0)
					for _, v := range strings.Split(value, " ") {
						val, _ := strconv.Atoi(v)
						s = append(s, val)
					}
					internalSizes[i] = s[0]
					sizes = append(sizes, s)
				}
				for j, t := range columnTypes {
					if t == "decimal" {
						precisions[j] = sizes[j][0]
						scales[j] = sizes[j][1]
					}
				}
			}

			qr.updateDescription(columnNames, columnTypes, displaySizes,
				internalSizes, precisions, scales, nullOks)
			qr.offset = 0
			qr.lastRowId = 0

		} else if strings.HasPrefix(line, mapi_MSG_PROMPT) {
			return nil

		} else if strings.HasPrefix(line, mapi_MSG_ERROR) {
			return fmt.Errorf("Database error: %s", line[1:])

		}
	}

	return fmt.Errorf("Unknown state: %s", r)
}

func (qr *queryResults) updateDescription(
	columnNames, columnTypes []string, displaySizes,
	internalSizes, precisions, scales, nullOks []int) {

	d := make([]description, len(columnNames))
	for i, _ := range columnNames {
		desc := description{
			columnName:   columnNames[i],
			columnType:   columnTypes[i],
			displaySize:  displaySizes[i],
			internalSize: internalSizes[i],
			precision:    precisions[i],
			scale:        scales[i],
			nullOk:       nullOks[i],
		}
		d[i] = desc
	}

	qr.description = d
}

func (qr *queryResults) parseTuple(d string) ([]driver.Value, error) {
	items := strings.Split(d[1:len(d)-1], ",\t")
	if len(items) != len(qr.description) {
		return nil, fmt.Errorf("Length of row doesn't match header")
	}

	v := make([]driver.Value, len(items))
	for i, value := range items {
		vv, err := qr.convert(value, qr.description[i].columnType)
		if err != nil {
			return nil, err
		}
		v[i] = vv
	}
	return v, nil
}

func (qr *queryResults) convert(value, dataType string) (driver.Value, error) {
	val, err := convertToGo(value, dataType)
	return val, err
}
