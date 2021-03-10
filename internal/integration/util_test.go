//
// Copyright 2021 Johns Hopkins University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// +build integration

package integration

import (
	"bytes"
	"embed"
	"errors"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"time"
)

// returns true if the current time minus the start time is greater than the timeout duration
func timedout(start time.Time, timeout time.Duration) bool {
	isTimedOut := time.Now().Sub(start) > timeout
	return isTimedOut
}

func perform(req *http.Request, assertStatusCode int) error {
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err := httpClient.Do(req); err != nil {
		return err
	} else {
		defer res.Body.Close()
		bod := &bytes.Buffer{}
		io.Copy(bod, res.Body)
		if res.StatusCode != assertStatusCode {
			return errors.New("Error creating container, " + res.Status + ": " + bod.String())
		}
	}

	return nil
}

func performWithHook(req *http.Request, bodyHook func(statusCode int, body io.Reader) error) error {
	req.SetBasicAuth(environment.FcrepoUser, environment.FcrepoPassword)
	if res, err := httpClient.Do(req); err != nil {
		return err
	} else {
		defer res.Body.Close()
		return bodyHook(res.StatusCode, res.Body)
	}
}

func testResources(shellGlob string, embeddedFs embed.FS) []fs.DirEntry {
	var matches []fs.DirEntry

	if entries, err := embeddedFs.ReadDir("."); err != nil {
		log.Fatalf("Error listing PASS test resources: %s", err.Error())
	} else {
		for _, entry := range entries {
			if matched, _ := path.Match(shellGlob, entry.Name()); matched {
				matches = append(matches, entry)
			}
		}
	}

	return matches
}
